use crate::zmq_helpers::{is_begin_marker, is_hello_marker, maybe_pin_for_bench, next_save_id, save_histogram_hgrm, save_raw_latencies, BoxError, HdrResultExt, JoinResultExt, ZmqResultExt};
use hdrhistogram::Histogram;
use std::arch::x86_64::_rdtsc;
use tokio::sync::oneshot;
use zmq::Context;

#[derive(Debug, Clone)]
pub struct Args {
    pub addresses: Vec<String>,
    pub num_messages: usize,
    pub payload_size: usize,
}

pub async fn run_async(args: Args, tsc_per_ns: f64, mut hello_ack_tx: Option<oneshot::Sender<()>>, mut begin_ack_tx: Option<oneshot::Sender<()>>) -> Result<Histogram<u64>, BoxError> {
    tokio::task::spawn_blocking(move || {
        maybe_pin_for_bench();
        let context = Context::new();
        let subscriber = context.socket(zmq::SUB).box_err()?;
        // HWM = num + headroom so drops impossible (hellos + benchmark msgs).
        let hwm = (args.num_messages + 1000) as i32;
        subscriber.set_rcvhwm(hwm).box_err()?;

        subscriber.set_subscribe(b"").box_err()?;

        for address in &args.addresses {
            subscriber.connect(address).box_err()?;
        }

        // Use a long timeout during synchronization phase drain (variable duration
        // depending on #receivers, scheduling, etc.). Set the short collection
        // timeout only for the actual benchmark phase loop below. This reduces
        // the chance of timeout during hello/await-begin (which could leak the
        // BEGIN marker into the "clean" measurement collection).
        subscriber.set_rcvtimeo(30000).box_err()?; // 30s for sync drain

        let mut recv_buffer = vec![0u8; args.payload_size];
        let mut latencies = Vec::with_capacity(args.num_messages);
        let mut recv_cpus: Vec<i32> = Vec::with_capacity(args.num_messages); // per-msg recv CPU (when SAVE_HISTS) to root-cause anomalies (core/CCD/scheduler vs index)

        // Synchronization phase drain (hellos + explicit BEGIN cutover):
        // Consume hellos (acking harness once). When BEGIN marker seen, consume it
        // as the signal that synchronization phase is complete. The subsequent
        // *exactly* num_messages in the clean loop below are the benchmark phase
        // data. That loop has no extra conditionals or phase logic at all.
        loop {
            match subscriber.recv_into(&mut recv_buffer, 0) {
                Ok(_) => {
                    if is_hello_marker(&recv_buffer) {
                        if hello_ack_tx.is_some() {
                            if let Some(tx) = hello_ack_tx.take() {
                                let _ = tx.send(());
                            }
                        }
                        continue;
                    }
                    if is_begin_marker(&recv_buffer) {
                        if let Some(tx) = begin_ack_tx.take() {
                            let _ = tx.send(());
                        }
                        break; // begin consumed; pure reals follow
                    }
                    // saw real tsc before begin (protocol edge); eat to keep the
                    // collection loop below getting exactly the intended reals
                    continue;
                }
                Err(_) => break,
            }
        }

        // Long timeout for collection phase to handle any gaps (no batch_sleep pacing now;
        // full speed means possible queuing but HWM headroom + handshake prevent drops).
        // If stalls >60s, break and warn.
        subscriber.set_rcvtimeo(60000).box_err()?; // 60s for benchmark phase

        // Clean benchmark phase -- the measured messages:
        // This is now a pure loop with only the necessary operations for the
        // benchmark: ZMQ recv, rdtsc, extract, push to pre-sized vec.
        // No hello tests, no begin tests, no harness channels, no extra branches
        // or syscalls/allocs in the per-message path.
        for _ in 0..args.num_messages {
            match subscriber.recv_into(&mut recv_buffer, 0) {
                Ok(_) => {
                    let recv_tsc = unsafe { _rdtsc() };
                    let sent_tsc = crate::zmq_helpers::extract_timestamp(&recv_buffer);
                    latencies.push(recv_tsc - sent_tsc);
                    #[cfg(target_os = "linux")]
                    unsafe { recv_cpus.push(libc::sched_getcpu()); }
                    #[cfg(not(target_os = "linux"))]
                    recv_cpus.push(-1);
                }
                Err(_) => break,
            }
        }

        let received_count = latencies.len();

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
            let transport = if args.addresses.first().map_or(false, |a| a.starts_with("ipc")) { "IPC" } else { "TCP" };
            let prefix = format!("indiv_PubSub_{}_{}_{}_{}", transport, args.payload_size, std::process::id(), save_id);
            let _ = save_raw_latencies(&prefix, &latencies_ns);
            let _ = save_histogram_hgrm(&prefix, &histogram);
            // per-msg recv CPUs for anomaly root-causing (lat vs core vs index)
            let cpus_path = format!("{}.recv_cpus.txt", prefix);
            let cpus_str: String = recv_cpus.iter().map(|c| format!("{}\n", c)).collect();
            let _ = std::fs::write(cpus_path, cpus_str);
        }

        if received_count < args.num_messages {
            eprintln!(
                "Warning: Received {}/{} messages (dropped {})",
                received_count,
                args.num_messages,
                args.num_messages - received_count
            );
        }

        Ok(histogram)
    })
    .await
    .join_err()
}
