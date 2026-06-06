use crate::zmq_helpers::{is_begin_marker, is_hello_marker, maybe_pin_for_bench, next_save_id, save_histogram_hgrm, save_raw_latencies, BoxError, HdrResultExt, JoinResultExt, ZmqResultExt};
use hdrhistogram::Histogram;
use std::arch::x86_64::_rdtsc;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;
use tokio::runtime::Handle;
use tokio::sync::{oneshot, Barrier};

use zmq::Context;

#[derive(Debug, Clone)]
pub struct Args {
    pub payload_size: usize,
    pub num_messages: usize,
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
        maybe_pin_for_bench();
        let context = Context::new();
        let receiver = context.socket(zmq::DEALER).box_err()?;

        let identity = format!("dealer_{}", receiver_id);
        receiver.set_identity(identity.as_bytes()).box_err()?;
        let hwm = (recv_args.num_messages + 1000) as i32;
        receiver.set_rcvhwm(hwm).box_err()?;

        receiver.connect(&recv_args.router_address).box_err()?;

        let mut hello_ack_tx = hello_ack_tx; // ensure mutable binding inside closure
        let mut recv_buffer = vec![0u8; recv_args.payload_size];
        let mut latencies = Vec::with_capacity(recv_args.num_messages);
        let mut recv_cpus: Vec<i32> = Vec::with_capacity(recv_args.num_messages); // per-msg recv CPU

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
            #[cfg(target_os = "linux")]
            unsafe { recv_cpus.push(libc::sched_getcpu()); }
            #[cfg(not(target_os = "linux"))]
            recv_cpus.push(-1);
        }

        let handle = Handle::current();
        handle.block_on(recv_end_barrier.wait());

        let latencies_ns: Vec<u64> = latencies
            .iter()
            .map(|&c| (c as f64 / tsc_per_ns) as u64)
            .collect();

        let mut histogram = Histogram::<u64>::new(3).box_err()?;
        for &latency_ns in &latencies_ns {
            histogram.record(latency_ns).box_err()?;
        }

        // Temporary saves for statistical smoke-test analysis (opt-in via env)
        if std::env::var("ZMQ_BENCH_SAVE_HISTS").is_ok() {
            let save_id = next_save_id();
            let transport = if recv_args.router_address.starts_with("ipc") { "IPC" } else { "TCP" };
            let prefix = format!("indiv_DealerRouter_{}_{}_{}_{}", transport, recv_args.payload_size, std::process::id(), save_id);
            let _ = save_raw_latencies(&prefix, &latencies_ns);
            let _ = save_histogram_hgrm(&prefix, &histogram);
            let cpus_path = format!("{}.recv_cpus.txt", prefix);
            let cpus_str: String = recv_cpus.iter().map(|c| format!("{}\n", c)).collect();
            let _ = std::fs::write(cpus_path, cpus_str);
        }

        Ok(histogram)
    });

    let send_args = args.clone();
    let send_handle = tokio::task::spawn_blocking(move || -> Result<(), BoxError> {
        maybe_pin_for_bench();
        let context = Context::new();
        let sender = context.socket(zmq::DEALER).box_err()?;

        let identity = format!("dealer_{}", sender_id);
        sender.set_identity(identity.as_bytes()).box_err()?;
        let hwm = (send_args.num_messages + 1000) as i32;
        sender.set_sndhwm(hwm).box_err()?;

        let router_addr = send_args.router_address.clone();
        sender.connect(&router_addr).box_err()?;

        let dest_id = format!("dealer_{}", target_receiver_id).into_bytes();
        let mut send_buffer = vec![0u8; send_args.payload_size];

        // No batch pacing sleeps. HWM set to num + headroom; full speed.


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

        // No settle sleep after BEGIN (see pubsub BEGIN ACK handshake and worktree
        // experiments): the synchronization phase + begin acks (where implemented)
        // ensure receivers are in clean phase. batch_sleep controls pacing for the
        // measurement. Settle removed per request (1ms was always chosen anyway).


        // Real benchmark phase. Hot path has no phase conditionals or extra recording.
        for _i in 0..send_args.num_messages {
            let send_tsc = unsafe { _rdtsc() };
            send_buffer[0..8].copy_from_slice(&send_tsc.to_le_bytes());

            sender.send(&dest_id, zmq::SNDMORE).box_err()?;
            sender.send(&send_buffer, 0).box_err()?;
        }

        Ok(())
    });

    send_handle.await.join_err()?;
    let histogram = recv_handle.await.join_err()?;

    Ok(histogram)
}
