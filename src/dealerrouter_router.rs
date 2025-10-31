use crate::zmq_helpers::{BoxError, JoinResultExt, ZmqResultExt};
use std::sync::Arc;
use tokio::sync::Barrier;
use zmq::Context;

#[derive(Debug, Clone)]
pub struct Args {
    pub bind_address: String,
    pub num_dealers: usize,
    pub num_messages_per_dealer: usize,
    pub hwm: i32,
}

pub async fn run_async(args: Args, end_barrier: Arc<Barrier>) -> Result<(), BoxError> {
    tokio::task::spawn_blocking(move || -> Result<(), BoxError> {
        let context = Context::new();
        let router = context.socket(zmq::ROUTER).box_err()?;

        router.set_sndhwm(args.hwm).box_err()?;
        router.set_rcvhwm(args.hwm).box_err()?;

        router.bind(&args.bind_address).box_err()?;

        let total_messages = args.num_dealers * args.num_messages_per_dealer;

        for _ in 0..total_messages {
            let _sender_id = router.recv_msg(0).box_err()?;
            let dest_id = router.recv_msg(0).box_err()?;
            let payload = router.recv_msg(0).box_err()?;

            router.send(dest_id, zmq::SNDMORE).box_err()?;
            router.send(payload, 0).box_err()?;
        }

        let handle = tokio::runtime::Handle::current();
        handle.block_on(end_barrier.wait());

        if args.bind_address.starts_with("ipc://") {
            let path = args.bind_address.trim_start_matches("ipc://");
            let _ = std::fs::remove_file(path);
        }

        Ok(())
    })
    .await
    .join_err()
}
