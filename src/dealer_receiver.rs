use crate::zmq_helpers::{is_begin_marker, is_hello_marker, maybe_pin_for_bench, next_save_id, save_histogram_hgrm, save_raw_latencies, BoxError, HdrResultExt, JoinResultExt, ZmqResultExt};
use hdrhistogram::Histogram;
use std::arch::x86_64::_rdtsc;
use tokio::sync::oneshot;
use zmq::Context;

#[derive(Debug, Clone)]
pub struct Args {
    pub payload_size: usize,
    pub num_messages: usize,
    pub bind_address: String,
}

pub async fn run_async(args: Args, tsc_per_ns: f64, mut hello_ack_tx: Option<oneshot::Sender<()>>) -> Result<Histogram<u64>, BoxError> {
    tokio::task::spawn_blocking(move || {
        maybe_pin_for_bench();
        let context = Context::new();
        let dealer = context.socket(zmq::DEALER).box_err()?;

        let hwm = (args.num_messages + 1000) as i32;
        dealer.set_rcvhwm(hwm).box_err()?;
        dealer.set_sndhwm(hwm).box_err()?;

        dealer.bind(&args.bind_address).box_err()?;

        let mut recv_buffer = vec![0u8; args.payload_size];
        let ack = vec![0u8; 8];
        let mut latencies = Vec::with_capacity(args.num_messages);
        let mut recv_cpus: Vec<i32> = Vec::with_capacity(args.num_messages); // per-msg recv CPU tooling

        // Synchronization phase drain (hellos + explicit begin cutover):
        // Reply on *every* message during drain (hellos and the begin) to keep the
        // sender's send+recv-reply loop from blocking. Ack harness only on first hello.
        // When begin marker seen, reply and cut over. The for loop below then receives
        // exactly the num real benchmark phase messages with a completely clean path.
        loop {
            dealer.recv_into(&mut recv_buffer, 0).box_err()?;

            if is_hello_marker(&recv_buffer) {
                if hello_ack_tx.is_some() {
                    if let Some(tx) = hello_ack_tx.take() {
                        let _ = tx.send(());
                    }
                }
                dealer.send(&ack, 0).box_err()?; // keep sender progressing
                continue;
            }
            if is_begin_marker(&recv_buffer) {
                dealer.send(&ack, 0).box_err()?; // reply to the begin too
                break;
            }
            // real tsc before begin (edge): reply and eat to preserve count in collection
            dealer.send(&ack, 0).box_err()?;
            continue;
        }

        // Clean benchmark phase -- exactly num real messages.
        // No phase markers, no hello tests, no harness acks, no extra branches.
        // Only the ZMQ interactions required by the benchmark (recv + send ack)
        // plus the TSC work.
        for _ in 0..args.num_messages {
            dealer.recv_into(&mut recv_buffer, 0).box_err()?;

            let recv_tsc = unsafe { _rdtsc() };
            let sent_tsc = crate::zmq_helpers::extract_timestamp(&recv_buffer);

            latencies.push(recv_tsc - sent_tsc);
            #[cfg(target_os = "linux")]
            unsafe { recv_cpus.push(libc::sched_getcpu()); }
            #[cfg(not(target_os = "linux"))]
            recv_cpus.push(-1);

            // ACK removed in clean phase (per allowance for 1M <1s speed; was ~24s with ACKs).
            // Sync handshake (hello + begin) still acked for stability. HWM headroom prevents drops.
            // dealer.send(&ack, 0).box_err()?;
        }

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
            let transport = if args.bind_address.starts_with("ipc") { "IPC" } else { "TCP" };
            let prefix = format!("indiv_Dealer_{}_{}_{}_{}", transport, args.payload_size, std::process::id(), save_id);
            let _ = save_raw_latencies(&prefix, &latencies_ns);
            let _ = save_histogram_hgrm(&prefix, &histogram);
            let cpus_path = format!("{}.recv_cpus.txt", prefix);
            let cpus_str: String = recv_cpus.iter().map(|c| format!("{}\n", c)).collect();
            let _ = std::fs::write(cpus_path, cpus_str);
        }

        if args.bind_address.starts_with("ipc://") {
            let path = args.bind_address.trim_start_matches("ipc://");
            let _ = std::fs::remove_file(path);
        }

        Ok(histogram)
    })
    .await
    .join_err()
}
