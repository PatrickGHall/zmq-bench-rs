use crate::zmq_helpers::{BoxError, JoinResultExt, ZmqResultExt};
use std::arch::x86_64::_rdtsc;
use zmq::Context;

#[derive(Debug, Clone)]
pub struct Args {
    pub payload_size: usize,
    pub num_messages: usize,
    pub receiver_address: String,
}

pub async fn run_async(args: Args) -> Result<(), BoxError> {
    tokio::task::spawn_blocking(move || {
        let context = Context::new();
        let dealer = context.socket(zmq::DEALER).box_err()?;

        dealer.connect(&args.receiver_address).box_err()?;
        let mut send_buffer = vec![0u8; args.payload_size];
        let mut ack_buffer = vec![0u8; 8];

        for _ in 0..args.num_messages {
            let send_tsc = unsafe { _rdtsc() };
            send_buffer[0..8].copy_from_slice(&send_tsc.to_le_bytes());

            dealer.send(&send_buffer, 0).box_err()?;
            dealer.recv_into(&mut ack_buffer, 0).box_err()?;
        }

        Ok(())
    })
    .await
    .join_err()
}
