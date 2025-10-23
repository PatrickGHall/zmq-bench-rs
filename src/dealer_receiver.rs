use crate::zmq_helpers::{BoxError, HdrResultExt, JoinResultExt, ZmqResultExt};
use hdrhistogram::serialization::Serializer;
use hdrhistogram::Histogram;
use std::arch::x86_64::_rdtsc;
use std::fs::File;
use zmq::Context;

#[derive(Debug, Clone)]
pub struct Args {
    pub payload_size: usize,
    pub num_messages: usize,
    pub bind_address: String,
    pub id: usize,
    pub benchmark_name: String,
}

fn extract_timestamp(buffer: &[u8]) -> u64 {
    u64::from_le_bytes([
        buffer[0], buffer[1], buffer[2], buffer[3],
        buffer[4], buffer[5], buffer[6], buffer[7],
    ])
}

pub async fn run_async(args: Args, tsc_per_ns: f64) -> Result<(), BoxError> {
    tokio::task::spawn_blocking(move || {

        let context = Context::new();
        let dealer = context.socket(zmq::DEALER).box_err()?;

        dealer.bind(&args.bind_address).box_err()?;

        let mut recv_buffer = vec![0u8; args.payload_size];
        let ack = vec![0u8; 8];
        let mut latencies = Vec::with_capacity(args.num_messages);

        for _ in 0..args.num_messages {
            dealer.recv_into(&mut recv_buffer, 0).box_err()?;

            let recv_tsc = unsafe { _rdtsc() };
            let sent_tsc = extract_timestamp(&recv_buffer);

            latencies.push(recv_tsc - sent_tsc);

            dealer.send(&ack, 0).box_err()?;
        }

        let mut histogram = Histogram::<u64>::new(3).box_err()?;

        for latency_cycles in latencies {
            let latency_ns = (latency_cycles as f64 / tsc_per_ns) as u64;
            histogram.record(latency_ns).box_err()?;
        }

        let filename = format!(
            "{}_{}_{}.hgrm",
            args.benchmark_name,
            args.payload_size,
            format!("receiver_{}", args.id)
        );
        let mut file = File::create(&filename)?;
        let mut serializer = hdrhistogram::serialization::V2Serializer::new();
        serializer.serialize(&histogram, &mut file).box_err()?;

        if args.bind_address.starts_with("ipc://") {
            let path = args.bind_address.trim_start_matches("ipc://");
            let _ = std::fs::remove_file(path);
        }

        Ok(())
    })
    .await
    .join_err()
}
