use hdrhistogram::serialization::Serializer;
use hdrhistogram::Histogram;
use std::fs::File;
use zmq::Context;

#[derive(Debug, Clone)]
pub struct Args {
    pub addresses: Vec<String>,
    pub num_messages: usize,
    pub id: String,
    pub benchmark_name: String,
    pub payload_size: usize,
    pub hwm: Option<i32>,
}

fn extract_timestamp(buffer: &[u8]) -> u64 {
    u64::from_le_bytes([
        buffer[0], buffer[1], buffer[2], buffer[3],
        buffer[4], buffer[5], buffer[6], buffer[7],
    ])
}

pub async fn run_async(
    args: Args,
    tsc_per_ns: f64,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tokio::task::spawn_blocking(move || {
        let context = Context::new();
        let subscriber = context.socket(zmq::SUB).map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
            Box::new(std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
        })?;

        if let Some(hwm) = args.hwm {
            subscriber.set_rcvhwm(hwm).map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
                Box::new(std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
            })?;
        }

        for address in &args.addresses {
            subscriber.connect(address).map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
                Box::new(std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
            })?;
        }
        subscriber.set_subscribe(b"").map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
            Box::new(std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
        })?;

        const RECEIVE_TIMEOUT_MS: i32 = 5000;
        subscriber.set_rcvtimeo(RECEIVE_TIMEOUT_MS).map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
            Box::new(std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
        })?;

        let mut recv_buffer = vec![0u8; args.payload_size];
        let mut latencies = Vec::with_capacity(args.num_messages);

        for _ in 0..args.num_messages {
            match subscriber.recv_into(&mut recv_buffer, 0) {
                Ok(_) => {
                    let recv_tsc = unsafe { std::arch::x86_64::_rdtsc() };
                    let sent_tsc = extract_timestamp(&recv_buffer);
                    latencies.push(recv_tsc - sent_tsc);
                }
                Err(_) => break,
            }
        }

        let received_count = latencies.len();

        let mut histogram = Histogram::<u64>::new(3).map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
            Box::new(std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
        })?;
        for latency_cycles in latencies {
            let latency_ns = (latency_cycles as f64 / tsc_per_ns) as u64;
            histogram.record(latency_ns).map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
                Box::new(std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
            })?;
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
        let mut file = File::create(&filename).map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
            Box::new(e)
        })?;
        let mut serializer = hdrhistogram::serialization::V2Serializer::new();
        serializer.serialize(&histogram, &mut file).map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
            Box::new(std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
        })?;

        Ok(())
    })
    .await
    .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
        Box::new(std::io::Error::new(
            std::io::ErrorKind::Other,
            format!("Task join error: {}", e),
        ))
    })?
}
