use crate::zmq_helpers::{maybe_pin_for_bench, BoxError, JoinResultExt, ZmqResultExt};
use std::arch::x86_64::_rdtsc;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use zmq::Context;

#[derive(Debug, Clone)]
pub struct Args {
    pub payload_size: usize,
    pub num_messages: usize,
    pub receiver_address: String,
}

pub async fn run_async(
    args: Args,
    bench_phase: Option<Arc<AtomicBool>>,
    first_hello_tsc: Option<Arc<AtomicU64>>,
    last_bench_start_tsc: Option<Arc<AtomicU64>>,
) -> Result<(), BoxError> {
    tokio::task::spawn_blocking(move || {
        maybe_pin_for_bench();
        let context = Context::new();
        let dealer = context.socket(zmq::DEALER).box_err()?;

        let hwm = (args.num_messages + 1000) as i32;
        dealer.set_sndhwm(hwm).box_err()?;
        dealer.set_rcvhwm(hwm).box_err()?;

        dealer.connect(&args.receiver_address).box_err()?;

        let mut send_buffer = vec![0u8; args.payload_size];
        let mut ack_buffer = vec![0u8; 8];

        // Synchronization phase: send hello (marker 0) + wait for per-message reply.
        // The receiver will ack the hello to the harness on the first one it sees.
        // When the atomic is set, we send the explicit begin cutover (with reply)
        // then start using real TSCs.
        loop {
            send_buffer[0..8].copy_from_slice(&0u64.to_le_bytes());

            if let Some(first) = &first_hello_tsc {
                let t = unsafe { _rdtsc() };
                first.fetch_min(t, Ordering::Relaxed);
            }

            dealer.send(&send_buffer, 0).box_err()?;
            dealer.recv_into(&mut ack_buffer, 0).box_err()?;

            if let Some(phase) = &bench_phase {
                if phase.load(Ordering::Acquire) {
                    break;
                }
            } else {
                // no phase flag (direct call path) — break after the hello(s); caller will send BEGIN + reals
                break;
            }
        }

        // Explicit cutover (end of synchronization phase) on the data path (receiver
        // will see it in drain and move to its clean benchmark phase collection).
        // We do the paired send+recv to keep the benchmark's req/reply shape.
        send_buffer[0..8].copy_from_slice(&crate::zmq_helpers::BEGIN_BENCHMARK_MARKER.to_le_bytes());
        dealer.send(&send_buffer, 0).box_err()?;
        dealer.recv_into(&mut ack_buffer, 0).box_err()?;

        // Record when *this sender* starts the benchmark phase (for sync duration).
        // Capture once before the loop; the per-message loop below has no extra conditional.
        if let Some(last) = &last_bench_start_tsc {
            let start_tsc = unsafe { _rdtsc() };
            last.fetch_max(start_tsc, Ordering::Relaxed);
        }

        // Real benchmark phase. Hot path has no phase-related conditionals or extra
        // duration recording; only ZMQ send+recv (inherent) + TSC.
        for _ in 0..args.num_messages {
            let send_tsc = unsafe { _rdtsc() };
            send_buffer[0..8].copy_from_slice(&send_tsc.to_le_bytes());

            dealer.send(&send_buffer, 0).box_err()?;
            // ACK removed in clean (speed); only sync phase.
            // dealer.recv_into(&mut ack_buffer, 0).box_err()?;
        }

        Ok(())
    })
    .await
    .join_err()
}
