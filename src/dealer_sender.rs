use std::sync::Arc;
use tokio::sync::Barrier;
use zmq::Context;

#[derive(Debug, Clone)]
pub struct Args {
    pub payload_size: usize,
    pub num_messages: usize,
    pub receiver_address: String,
}

pub async fn run_async(
    args: Args,
    barrier: Arc<Barrier>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tokio::task::spawn_blocking(move || {
        let context = Context::new();
        let dealer = context.socket(zmq::DEALER).map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
            Box::new(std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
        })?;

        dealer.connect(&args.receiver_address).map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
            Box::new(std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
        })?;

        let handle = tokio::runtime::Handle::current();
        handle.block_on(barrier.wait());

        let mut send_buffer = vec![0u8; args.payload_size];
        let mut ack_buffer = vec![0u8; 8];

        for _ in 0..args.num_messages {
            let send_tsc = unsafe { std::arch::x86_64::_rdtsc() };
            send_buffer[0..8].copy_from_slice(&send_tsc.to_le_bytes());

            dealer.send(&send_buffer, 0).map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
                Box::new(std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
            })?;

            dealer.recv_into(&mut ack_buffer, 0).map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
                Box::new(std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
            })?;
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
