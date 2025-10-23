use crate::zmq_helpers::{BoxError, JoinResultExt, ZmqResultExt};
use std::arch::x86_64::_rdtsc;
use std::sync::Arc;
use tokio::runtime::Handle;
use tokio::sync::Barrier;
use tokio::time::{sleep, Duration};
use zmq::Context;

#[derive(Debug, Clone)]
pub struct Args {
    pub payload_size: usize,
    pub num_messages: usize,
    pub address: String,
    pub hwm: Option<i32>,
    pub batch_sleep_ms: u64,
}

pub async fn run_async(args: Args, barrier: Arc<Barrier>) -> Result<(), BoxError> {
    tokio::task::spawn_blocking(move || {

        let context = Context::new();
        let publisher = context.socket(zmq::PUB).box_err()?;

        if let Some(hwm) = args.hwm {
            publisher.set_sndhwm(hwm).box_err()?;
        }

        publisher.bind(&args.address).box_err()?;

        let handle = Handle::current();
        handle.block_on(barrier.wait());

        handle.block_on(sleep(Duration::from_millis(100)));

        let mut msg = vec![0u8; args.payload_size];
        let batch_size = args.hwm.unwrap_or(1000) as usize;

        for i in 0..args.num_messages {
            let tsc = unsafe { _rdtsc() };
            msg[0..8].copy_from_slice(&tsc.to_le_bytes());

            publisher.send(&msg, 0).box_err()?;

            if args.batch_sleep_ms > 0 && (i + 1) % batch_size == 0 {
                handle.block_on(sleep(Duration::from_millis(args.batch_sleep_ms)));
            }
        }

        if args.address.starts_with("ipc://") {
            let path = args.address.trim_start_matches("ipc://");
            let _ = std::fs::remove_file(path);
        }

        Ok(())
    })
    .await
    .join_err()
}
