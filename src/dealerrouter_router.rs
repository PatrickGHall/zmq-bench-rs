use std::sync::Arc;
use tokio::sync::Barrier;
use zmq::Context;

#[derive(Debug, Clone)]
pub struct Args {
    pub bind_address: String,
    pub num_dealers: usize,
    pub num_messages_per_dealer: usize,
    pub payload_size: usize,
    pub hwm: Option<i32>,
}

pub async fn run_async(
    args: Args,
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

        router.bind(&args.bind_address).map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
            Box::new(std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
        })?;

        let total_messages = args.num_dealers * args.num_messages_per_dealer;
        let mut sender_id = zmq::Message::new();
        let mut dest_id = zmq::Message::new();
        let mut payload_buf = vec![0u8; args.payload_size];

        for _ in 0..total_messages {
            router.recv(&mut sender_id, 0).map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
                Box::new(std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
            })?;

            router.recv(&mut dest_id, 0).map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
                Box::new(std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
            })?;

            router.recv_into(&mut payload_buf, 0).map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
                Box::new(std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
            })?;

            router.send(&*dest_id, zmq::SNDMORE).map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
                Box::new(std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
            })?;

            router.send(&payload_buf, 0).map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
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
