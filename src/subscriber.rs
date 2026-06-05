use crate::zmq_helpers::{is_begin_marker, is_hello_marker, BoxError, HdrResultExt, JoinResultExt, ZmqResultExt};
use hdrhistogram::Histogram;
use std::arch::x86_64::_rdtsc;
use tokio::sync::oneshot;
use zmq::Context;

#[derive(Debug, Clone)]
pub struct Args {
    pub addresses: Vec<String>,
    pub num_messages: usize,
    pub payload_size: usize,
    pub hwm: i32,
}

pub async fn run_async(args: Args, tsc_per_ns: f64, mut hello_ack_tx: Option<oneshot::Sender<()>>) -> Result<Histogram<u64>, BoxError> {
    tokio::task::spawn_blocking(move || {
        let context = Context::new();
        let subscriber = context.socket(zmq::SUB).box_err()?;
        subscriber.set_rcvhwm(args.hwm).box_err()?;

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
                        break; // begin consumed; pure reals follow
                    }
                    // saw real tsc before begin (protocol edge); eat to keep the
                    // collection loop below getting exactly the intended reals
                    continue;
                }
                Err(_) => break,
            }
        }

        // Now set the short timeout for the benchmark collection phase.
        subscriber.set_rcvtimeo(5000).box_err()?; // 5s for benchmark phase

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
                }
                Err(_) => break,
            }
        }

        let received_count = latencies.len();

        let mut histogram = Histogram::<u64>::new(3).box_err()?;
        for latency_cycles in latencies {
            let latency_ns = (latency_cycles as f64 / tsc_per_ns) as u64;
            histogram.record(latency_ns).box_err()?;
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
