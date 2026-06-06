use crate::zmq_helpers::{maybe_pin_for_bench, BoxError, JoinResultExt, ZmqResultExt};
use std::arch::x86_64::_rdtsc;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use tokio::runtime::Handle;
use tokio::sync::oneshot;
use tokio::time::Duration;
use zmq::Context;

#[derive(Debug, Clone)]
pub struct Args {
    pub payload_size: usize,
    pub num_messages: usize,
    pub address: String,
}

pub async fn run_async(
    args: Args,
    bench_phase: Option<Arc<AtomicBool>>,
    first_hello_tsc: Option<Arc<AtomicU64>>,
    last_bench_start_tsc: Option<Arc<AtomicU64>>,
    reals_start_rx: Option<oneshot::Receiver<()>>,
) -> Result<(), BoxError> {
    tokio::task::spawn_blocking(move || {
        maybe_pin_for_bench();
        let context = Context::new();
        let publisher = context.socket(zmq::PUB).box_err()?;
        // HWM = num + headroom so drops impossible.
        let hwm = (args.num_messages + 1000) as i32;
        publisher.set_sndhwm(hwm).box_err()?;

        publisher.bind(&args.address).box_err()?;

        let handle = Handle::current();

        let mut msg = vec![0u8; args.payload_size];
        // No batch pacing. HWM = num + headroom; full speed after handshake.

        // Synchronization phase: send hello messages (first 8 bytes = 0) so that subscribers
        // can prove they are receiving on the data path and ack to the harness.
        // When the harness sets the atomic, we switch to real benchmark data.
        loop {
            // hello marker
            msg[0..8].copy_from_slice(&0u64.to_le_bytes());

            // Record the earliest hello send start time (for sync phase duration measurement)
            if let Some(first) = &first_hello_tsc {
                let t = unsafe { _rdtsc() };
                first.fetch_min(t, Ordering::Relaxed);
            }

            publisher.send(&msg, 0).box_err()?;

            if let Some(phase) = &bench_phase {
                if phase.load(Ordering::Acquire) {
                    break;
                }
            } else {
                // no phase flag (direct call path) — break after the hello(s); caller will send BEGIN + reals
                break;
            }
        }

        // Send explicit cutover marker (end of synchronization phase) on the data path.
        // Receivers will have consumed this in their pre-benchmark drain and then
        // entered their clean benchmark phase collection loop.
        msg[0..8].copy_from_slice(&crate::zmq_helpers::BEGIN_BENCHMARK_MARKER.to_le_bytes());
        publisher.send(&msg, 0).box_err()?;

        // Record when *this sender* starts the benchmark phase (for overall sync duration measurement).
        // Capture once, before the per-message loop. Hot path below has no extra conditional for it.
        if let Some(last) = &last_bench_start_tsc {
            let start_tsc = unsafe { _rdtsc() };
            last.fetch_max(start_tsc, Ordering::Relaxed);
        }

        // Wait for all subscribers to have received and ACKed the BEGIN marker
        // (signaled via the reals_start channel after the coordinator waits for
        // all begin_acks) before sending any real benchmark messages.
        // This completes the synchronization handshake on the receiver side.
        // No settle sleep is needed.
        if let Some(rx) = reals_start_rx {
            let _ = handle.block_on(async { rx.await });
        }

        // Real benchmark phase. Hot path (per-message) has no phase-related conditionals,
        // no extra TSC recording for duration, only ZMQ send + batch sleep if configured.
        for _i in 0..args.num_messages {
            let tsc = unsafe { _rdtsc() };
            msg[0..8].copy_from_slice(&tsc.to_le_bytes());

            publisher.send(&msg, 0).box_err()?;
        }

        if args.address.starts_with("ipc://") {
            let path = args.address.trim_start_matches("ipc://");
            let _ = std::fs::remove_file(path);
        }

        Ok(())
    })
    .await
    .join_err()
}
