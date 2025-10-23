use hdrhistogram::serialization::Serializer;
use hdrhistogram::Histogram;
use std::fs::File;
use std::sync::Arc;
use tokio::sync::Barrier;
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
    barrier: Arc<Barrier>,
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

        subscriber.set_subscribe(b"").map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
            Box::new(std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
        })?;

        let monitor_endpoint = format!("inproc://monitor-sub-{}", args.id);
        subscriber.monitor(&monitor_endpoint, zmq::SocketEvent::CONNECTED as i32).map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
            Box::new(std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
        })?;
        let monitor_socket = context.socket(zmq::PAIR).map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
            Box::new(std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
        })?;
        monitor_socket.connect(&monitor_endpoint).map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
            Box::new(std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
        })?;

        for address in &args.addresses {
            subscriber.connect(address).map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
                Box::new(std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
            })?;
        }

        let expected_connections = args.addresses.len();
        let mut connections_established = 0;
        while connections_established < expected_connections {
            let mut event_msg = zmq::Message::new();
            monitor_socket.recv(&mut event_msg, 0).map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
                Box::new(std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
            })?;
            let event_data = &event_msg;

            if event_data.len() >= 2 {
                let event_id = u16::from_le_bytes([event_data[0], event_data[1]]);
                let event = zmq::SocketEvent::from_raw(event_id);
                if event == zmq::SocketEvent::CONNECTED {
                    if monitor_socket.get_rcvmore().map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
                        Box::new(std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
                    })? {
                        let mut _endpoint_msg = zmq::Message::new();
                        monitor_socket.recv(&mut _endpoint_msg, 0).map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
                            Box::new(std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
                        })?;
                    }
                    connections_established += 1;
                }
            }
        }

        let handle = tokio::runtime::Handle::current();
        handle.block_on(barrier.wait());

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
