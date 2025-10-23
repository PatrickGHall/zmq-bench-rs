use crate::zmq_helpers::{BoxError, HdrResultExt, JoinResultExt, ZmqResultExt};
use hdrhistogram::serialization::Serializer;
use hdrhistogram::Histogram;
use std::arch::x86_64::_rdtsc;
use std::fs::File;
use std::sync::Arc;
use tokio::runtime::Handle;
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

pub async fn run_async(args: Args, tsc_per_ns: f64, barrier: Arc<Barrier>) -> Result<(), BoxError> {
    tokio::task::spawn_blocking(move || {

        let context = Context::new();
        let subscriber = context.socket(zmq::SUB).box_err()?;

        if let Some(hwm) = args.hwm {
            subscriber.set_rcvhwm(hwm).box_err()?;
        }

        subscriber.set_subscribe(b"").box_err()?;

        let monitor_endpoint = format!("inproc://monitor-sub-{}", args.id);
        subscriber
            .monitor(&monitor_endpoint, zmq::SocketEvent::CONNECTED as i32)
            .box_err()?;
        let monitor_socket = context.socket(zmq::PAIR).box_err()?;
        monitor_socket.connect(&monitor_endpoint).box_err()?;

        for address in &args.addresses {
            subscriber.connect(address).box_err()?;
        }

        let expected_connections = args.addresses.len();
        let mut connections_established = 0;
        while connections_established < expected_connections {
            let mut event_msg = zmq::Message::new();
            monitor_socket.recv(&mut event_msg, 0).box_err()?;
            let event_data = &event_msg;

            if event_data.len() >= 2 {
                let event_id = u16::from_le_bytes([event_data[0], event_data[1]]);
                let event = zmq::SocketEvent::from_raw(event_id);
                if event == zmq::SocketEvent::CONNECTED {
                    if monitor_socket.get_rcvmore().box_err()? {
                        let mut _endpoint_msg = zmq::Message::new();
                        monitor_socket.recv(&mut _endpoint_msg, 0).box_err()?;
                    }
                    connections_established += 1;
                }
            }
        }

        let handle = Handle::current();
        handle.block_on(barrier.wait());

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
