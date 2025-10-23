use crate::zmq_helpers::{BoxError, JoinResultExt, ZmqResultExt};
use std::sync::Arc;
use tokio::runtime::Handle;
use tokio::sync::Barrier;
use tokio::time::{sleep, Duration};
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
) -> Result<(), BoxError> {
    tokio::task::spawn_blocking(move || -> Result<(), BoxError> {
        let context = Context::new();
        let router = context.socket(zmq::ROUTER).box_err()?;

        if let Some(hwm) = args.hwm {
            router.set_sndhwm(hwm).box_err()?;
            router.set_rcvhwm(hwm).box_err()?;
        }

        let monitor_endpoint = "inproc://router-monitor";
        router
            .monitor(monitor_endpoint, zmq::SocketEvent::ACCEPTED as i32)
            .box_err()?;

        let monitor_socket = context.socket(zmq::PAIR).box_err()?;

        let handle = Handle::current();
        handle.block_on(sleep(Duration::from_millis(100)));

        monitor_socket.connect(monitor_endpoint).box_err()?;

        router.bind(&args.bind_address).box_err()?;

        let expected_connections = args.num_dealers * 2;
        let mut connections_accepted = 0;

        while connections_accepted < expected_connections {
            let mut event_msg = zmq::Message::new();
            monitor_socket.recv(&mut event_msg, 0).box_err()?;

            let event_data = &event_msg;
            if event_data.len() >= 2 {
                let event_id = u16::from_le_bytes([event_data[0], event_data[1]]);
                let event = zmq::SocketEvent::from_raw(event_id);
                if event == zmq::SocketEvent::ACCEPTED {
                    connections_accepted += 1;
                }
            }

            if monitor_socket.get_rcvmore().box_err()? {
                let mut _endpoint_msg = zmq::Message::new();
                monitor_socket.recv(&mut _endpoint_msg, 0).box_err()?;
            }
        }

        let handle = tokio::runtime::Handle::current();
        handle.block_on(start_barrier.wait());

        let total_messages = args.num_dealers * args.num_messages_per_dealer;

        for _ in 0..total_messages {
            let _sender_id = router.recv_msg(0).box_err()?;
            let dest_id = router.recv_msg(0).box_err()?;
            let payload = router.recv_msg(0).box_err()?;

            router.send(dest_id, zmq::SNDMORE).box_err()?;
            router.send(payload, 0).box_err()?;
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
    .join_err()
}
