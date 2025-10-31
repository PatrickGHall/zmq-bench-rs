use crate::zmq_helpers::{BoxError, HdrResultExt, JoinResultExt, ZmqResultExt};
use hdrhistogram::serialization::Serializer;
use hdrhistogram::Histogram;
use std::arch::x86_64::_rdtsc;
use std::fs::File;
use zmq::Context;

#[derive(Debug, Clone)]
pub struct Args {
    pub addresses: Vec<String>,
    pub num_messages: usize,
    pub id: String,
    pub benchmark_name: String,
    pub payload_size: usize,
    pub hwm: i32,
}

fn extract_timestamp(buffer: &[u8]) -> u64 {
    u64::from_le_bytes([
        buffer[0], buffer[1], buffer[2], buffer[3], buffer[4], buffer[5], buffer[6], buffer[7],
    ])
}

pub async fn run_async(args: Args, tsc_per_ns: f64) -> Result<(), BoxError> {
    tokio::task::spawn_blocking(move || {
        let context = Context::new();
        let subscriber = context.socket(zmq::SUB).box_err()?;
        subscriber.set_rcvhwm(args.hwm).box_err()?;

        subscriber.set_subscribe(b"").box_err()?;

        for address in &args.addresses {
            subscriber.connect(address).box_err()?;
        }

        const RECEIVE_TIMEOUT_MS: i32 = 5000;
        subscriber.set_rcvtimeo(RECEIVE_TIMEOUT_MS).box_err()?;

        let mut recv_buffer = vec![0u8; args.payload_size];
        let mut latencies = Vec::with_capacity(args.num_messages);

        for _ in 0..args.num_messages {
            match subscriber.recv_into(&mut recv_buffer, 0) {
                Ok(_) => {
                    let recv_tsc = unsafe { _rdtsc() };
                    let sent_tsc = extract_timestamp(&recv_buffer);
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

        let filename = format!(
            "{}_{}_{}.hgrm",
            args.benchmark_name, args.payload_size, args.id
        );
        let mut file = File::create(&filename)?;
        let mut serializer = hdrhistogram::serialization::V2Serializer::new();
        serializer.serialize(&histogram, &mut file).box_err()?;

        Ok(())
    })
    .await
    .join_err()
}
