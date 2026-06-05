use crate::zmq_helpers::{is_begin_marker, is_hello_marker, BoxError, HdrResultExt, JoinResultExt, ZmqResultExt};
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
        let context = Context::new();
        let dealer = context.socket(zmq::DEALER).box_err()?;

        dealer.bind(&args.bind_address).box_err()?;

        let mut recv_buffer = vec![0u8; args.payload_size];
        let ack = vec![0u8; 8];
        let mut latencies = Vec::with_capacity(args.num_messages);

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

            dealer.send(&ack, 0).box_err()?;
        }

        let mut histogram = Histogram::<u64>::new(3).box_err()?;

        for latency_cycles in latencies {
            let latency_ns = (latency_cycles as f64 / tsc_per_ns) as u64;
            histogram.record(latency_ns).box_err()?;
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
