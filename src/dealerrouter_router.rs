use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Barrier;
use zmq::Context;

#[derive(Debug, Clone)]
pub struct Args {
    pub bind_address: String,
    pub num_dealers: usize,
    pub num_messages_per_dealer: usize,
    pub hwm: Option<i32>,
}

pub async fn run_async(
    args: Args,
    start_barrier: Arc<Barrier>,
    receiver_barrier: Arc<Barrier>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tokio::task::spawn_blocking(move || -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let context = Context::new();
        let router = context.socket(zmq::ROUTER).map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
            Box::new(std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
        })?;

        if let Some(hwm) = args.hwm {
            router.set_sndhwm(hwm).map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
                Box::new(std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
            })?;
            router.set_rcvhwm(hwm).map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
                Box::new(std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
            })?;
        }

        let monitor_endpoint = "inproc://router-monitor";
        router.monitor(monitor_endpoint, zmq::SocketEvent::ACCEPTED as i32).map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
            Box::new(std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
        })?;

        let monitor_socket = context.socket(zmq::PAIR).map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
            Box::new(std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
        })?;

        std::thread::sleep(Duration::from_millis(100));

        monitor_socket.connect(monitor_endpoint).map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
            Box::new(std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
        })?;

        router.bind(&args.bind_address).map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
            Box::new(std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
        })?;

        let expected_connections = args.num_dealers * 2;
        let mut connections_accepted = 0;

        while connections_accepted < expected_connections {
            let mut event_msg = zmq::Message::new();
            monitor_socket.recv(&mut event_msg, 0).map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
                Box::new(std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
            })?;

            let event_data = &event_msg;
            if event_data.len() >= 2 {
                let event_id = u16::from_le_bytes([event_data[0], event_data[1]]);
                let event = zmq::SocketEvent::from_raw(event_id);
                if event == zmq::SocketEvent::ACCEPTED {
                    connections_accepted += 1;
                }
            }

            if monitor_socket.get_rcvmore().map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
                Box::new(std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
            })? {
                let mut _endpoint_msg = zmq::Message::new();
                monitor_socket.recv(&mut _endpoint_msg, 0).map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
                    Box::new(std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
                })?;
            }
        }

        let handle = tokio::runtime::Handle::current();
        handle.block_on(start_barrier.wait());

        let total_messages = args.num_dealers * args.num_messages_per_dealer;

        for _ in 0..total_messages {
            let _sender_id = router.recv_msg(0).map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
                Box::new(std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
            })?;
            let dest_id = router.recv_msg(0).map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
                Box::new(std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
            })?;
            let payload = router.recv_msg(0).map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
                Box::new(std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
            })?;

            router.send(dest_id, zmq::SNDMORE).map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
                Box::new(std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
            })?;
            router.send(payload, 0).map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
                Box::new(std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
            })?;
        }

        let handle = tokio::runtime::Handle::current();
        handle.block_on(receiver_barrier.wait());

        if args.bind_address.starts_with("ipc://") {
            let path = args.bind_address.trim_start_matches("ipc://");
            let _ = std::fs::remove_file(path);
        }

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
