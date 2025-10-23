use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Barrier;
use zmq::Context;

#[derive(Debug, Clone)]
pub struct Args {
    pub payload_size: usize,
    pub num_messages: usize,
    pub address: String,
    pub hwm: Option<i32>,
    pub batch_sleep_ms: u64,
}

pub async fn run_async(
    args: Args,
    barrier: Arc<Barrier>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tokio::task::spawn_blocking(move || {
        let context = Context::new();
        let publisher =
            context
                .socket(zmq::PUB)
                .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
                    Box::new(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        e.to_string(),
                    ))
                })?;

        if let Some(hwm) = args.hwm {
            publisher
                .set_sndhwm(hwm)
                .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
                    Box::new(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        e.to_string(),
                    ))
                })?;
        }

        publisher
            .bind(&args.address)
            .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
                Box::new(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    e.to_string(),
                ))
            })?;

        let handle = tokio::runtime::Handle::current();
        handle.block_on(barrier.wait());

        //TODO: seems like subscriptions take time to register with publishers.
        // There may be a socket event we can listen to in order to know that
        // everyone is ready to roll, rather than this sleep.
        std::thread::sleep(Duration::from_millis(100));

        let mut msg = vec![0u8; args.payload_size];
        let batch_size = args.hwm.unwrap_or(1000) as usize;

        for i in 0..args.num_messages {
            let tsc = unsafe { std::arch::x86_64::_rdtsc() };
            msg[0..8].copy_from_slice(&tsc.to_le_bytes());

            publisher
                .send(&msg, 0)
                .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
                    Box::new(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        e.to_string(),
                    ))
                })?;

            if args.batch_sleep_ms > 0 && (i + 1) % batch_size == 0 {
                std::thread::sleep(Duration::from_millis(args.batch_sleep_ms));
            }
        }

        if args.address.starts_with("ipc://") {
            let path = args.address.trim_start_matches("ipc://");
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
