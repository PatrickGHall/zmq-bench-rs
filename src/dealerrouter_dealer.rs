use crate::zmq_helpers::{is_begin_marker, is_hello_marker, BoxError, HdrResultExt, JoinResultExt, ZmqResultExt};
use hdrhistogram::Histogram;
use std::arch::x86_64::_rdtsc;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;
use tokio::runtime::Handle;
use tokio::sync::{oneshot, Barrier};
use tokio::time::sleep;
use zmq::Context;

#[derive(Debug, Clone)]
pub struct Args {
    pub payload_size: usize,
    pub num_messages: usize,
    pub hwm: i32,
    pub batch_sleep_ms: u64,
    pub router_address: String,
    pub dealer_id: usize,
    pub num_dealers: usize,
}

pub async fn run_async(
    args: Args,
    hello_ack_tx: oneshot::Sender<()>,
    bench_phase: Option<Arc<AtomicBool>>,
    first_hello_tsc: Option<Arc<AtomicU64>>,
    last_bench_start_tsc: Option<Arc<AtomicU64>>,
    end_barrier: Arc<Barrier>,
    tsc_per_ns: f64,
) -> Result<Histogram<u64>, BoxError> {
    let sender_id = args.dealer_id * 2;
    let receiver_id = args.dealer_id * 2 + 1;
    let target_receiver_id = ((args.dealer_id + 1) % args.num_dealers) * 2 + 1;

    let recv_args = args.clone();
    let hello_ack_tx = Some(hello_ack_tx);
    let recv_end_barrier = end_barrier.clone();
    let recv_handle = tokio::task::spawn_blocking(move || -> Result<Histogram<u64>, BoxError> {
        let context = Context::new();
        let receiver = context.socket(zmq::DEALER).box_err()?;

        let identity = format!("dealer_{}", receiver_id);
        receiver.set_identity(identity.as_bytes()).box_err()?;
        receiver.set_rcvhwm(args.hwm).box_err()?;

        receiver.connect(&recv_args.router_address).box_err()?;

        let mut hello_ack_tx = hello_ack_tx; // ensure mutable binding inside closure
        let mut recv_buffer = vec![0u8; recv_args.payload_size];
        let mut latencies = Vec::with_capacity(recv_args.num_messages);

        // Synchronization phase drain (hellos + explicit begin cutover):
        // Eat hellos (notify harness once on first). Consume the begin marker as
        // the cutover signal from the sender. Then the loop below receives exactly
        // num real benchmark phase messages on a path with no extra conditionals at all.
        loop {
            receiver.recv_into(&mut recv_buffer, 0).box_err()?;

            if is_hello_marker(&recv_buffer) {
                if hello_ack_tx.is_some() {
                    if let Some(tx) = hello_ack_tx.take() {
                        let _ = tx.send(());
                    }
                }
                // eat hello; do not record
                continue;
            }
            if is_begin_marker(&recv_buffer) {
                break; // cutover consumed; pure reals follow
            }
            // real before begin (edge): eat to keep the collection count exact
            continue;
        }

        // Clean benchmark phase: exactly num real messages.
        // No hello/begin tests, no harness notifications, no extra branches or
        // costs beyond ZMQ recv + the benchmark TSC work.
        for _ in 0..recv_args.num_messages {
            receiver.recv_into(&mut recv_buffer, 0).box_err()?;

            let recv_tsc = unsafe { _rdtsc() };
            let sent_tsc = crate::zmq_helpers::extract_timestamp(&recv_buffer);

            latencies.push(recv_tsc - sent_tsc);
        }

        let handle = Handle::current();
        handle.block_on(recv_end_barrier.wait());

        let mut histogram = Histogram::<u64>::new(3).box_err()?;

        for latency_cycles in latencies {
            let latency_ns = (latency_cycles as f64 / tsc_per_ns) as u64;
            histogram.record(latency_ns).box_err()?;
        }

        Ok(histogram)
    });

    let send_args = args.clone();
    let send_handle = tokio::task::spawn_blocking(move || -> Result<(), BoxError> {
        let context = Context::new();
        let sender = context.socket(zmq::DEALER).box_err()?;

        let identity = format!("dealer_{}", sender_id);
        sender.set_identity(identity.as_bytes()).box_err()?;
        sender.set_sndhwm(args.hwm).box_err()?;

        let router_addr = send_args.router_address.clone();
        sender.connect(&router_addr).box_err()?;

        let dest_id = format!("dealer_{}", target_receiver_id).into_bytes();
        let mut send_buffer = vec![0u8; send_args.payload_size];

        let batch_size = args.hwm as usize;

        // Synchronization phase: keep sending hello messages (timestamp field = 0) until the
        // harness signals (via the atomic) that all relevant receivers have seen
        // (and acked) at least one hello on the data path. Then send explicit
        // cutover begin marker, then real benchmark phase data.
        loop {
            // hello marker
            send_buffer[0..8].copy_from_slice(&0u64.to_le_bytes());

            if let Some(first) = &first_hello_tsc {
                let t = unsafe { _rdtsc() };
                first.fetch_min(t, Ordering::Relaxed);
            }

            sender.send(&dest_id, zmq::SNDMORE).box_err()?;
            sender.send(&send_buffer, 0).box_err()?;

            if let Some(phase) = &bench_phase {
                if phase.load(Ordering::Acquire) {
                    break;
                }
            } else {
                // no phase flag (direct call path) — break after the hello(s); caller will send BEGIN + reals
                break;
            }
        }

        // Explicit begin cutover (end of synchronization phase; receivers drain it
        // before entering their clean collection of the benchmark phase messages).
        send_buffer[0..8].copy_from_slice(&crate::zmq_helpers::BEGIN_BENCHMARK_MARKER.to_le_bytes());
        sender.send(&dest_id, zmq::SNDMORE).box_err()?;
        sender.send(&send_buffer, 0).box_err()?;

        // Record when *this sender* starts the benchmark phase (for sync duration).
        // Capture once before the loop; hot path below has no extra conditional.
        if let Some(last) = &last_bench_start_tsc {
            let start_tsc = unsafe { _rdtsc() };
            last.fetch_max(start_tsc, Ordering::Relaxed);
        }

        // Real benchmark phase. Hot path has no phase conditionals or extra recording.
        for i in 0..send_args.num_messages {
            let send_tsc = unsafe { _rdtsc() };
            send_buffer[0..8].copy_from_slice(&send_tsc.to_le_bytes());

            sender.send(&dest_id, zmq::SNDMORE).box_err()?;
            sender.send(&send_buffer, 0).box_err()?;

            if send_args.batch_sleep_ms > 0 && (i + 1) % batch_size == 0 {
                let handle = Handle::current();
                handle.block_on(sleep(Duration::from_millis(send_args.batch_sleep_ms)));
            }
        }

        Ok(())
    });

    send_handle.await.join_err()?;
    let histogram = recv_handle.await.join_err()?;

    Ok(histogram)
}
