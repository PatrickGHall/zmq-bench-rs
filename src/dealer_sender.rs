use crate::zmq_helpers::{connect_and_wait, BoxError, JoinResultExt, ZmqResultExt};
use std::arch::x86_64::_rdtsc;
use std::sync::Arc;
use tokio::runtime::Handle;
use tokio::sync::Barrier;
use zmq::Context;

#[derive(Debug, Clone)]
pub struct Args {
    pub payload_size: usize,
    pub num_messages: usize,
    pub receiver_address: String,
    pub id: usize,
}

pub async fn run_async(args: Args, barrier: Arc<Barrier>) -> Result<(), BoxError> {
    tokio::task::spawn_blocking(move || {
        let context = Context::new();
        let dealer = context.socket(zmq::DEALER).box_err()?;

        let handle = Handle::current();
        handle.block_on(barrier.wait());

        let recv_addr = args.receiver_address.clone();
        connect_and_wait(
            &context,
            &dealer,
            &format!("dealer-sender-{}", args.id),
            |socket| socket.connect(&recv_addr).box_err(),
        )?;

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
