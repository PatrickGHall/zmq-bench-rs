use crate::zmq_helpers::{connect_and_wait, BoxError, JoinResultExt, ZmqResultExt};
use std::sync::Arc;
use tokio::runtime::Handle;
use tokio::sync::Barrier;
use tokio::time::{sleep, Duration};
use zmq::Context;

#[derive(Debug, Clone)]
pub struct Args {
    pub frontend_address: String,
    pub backend_address: String,
    pub num_messages: usize,
    pub hwm: Option<i32>,
}

pub async fn run_async(
    args: Args,
    start_barrier: Arc<Barrier>,
    end_barrier: Arc<Barrier>,
) -> Result<(), BoxError> {
    tokio::task::spawn_blocking(move || -> Result<(), BoxError> {
        let context = Context::new();
        let frontend = context.socket(zmq::ROUTER).box_err()?;
        let backend = context.socket(zmq::DEALER).box_err()?;

        if let Some(hwm) = args.hwm {
            frontend.set_sndhwm(hwm).box_err()?;
            frontend.set_rcvhwm(hwm).box_err()?;
            backend.set_sndhwm(hwm).box_err()?;
            backend.set_rcvhwm(hwm).box_err()?;
        }

        let frontend_monitor_endpoint = "inproc://proxy-frontend-monitor";
        frontend
            .monitor(frontend_monitor_endpoint, zmq::SocketEvent::ACCEPTED as i32)
            .box_err()?;

        let frontend_monitor = context.socket(zmq::PAIR).box_err()?;

        let handle = Handle::current();
        handle.block_on(sleep(Duration::from_millis(100)));

        frontend_monitor.connect(frontend_monitor_endpoint).box_err()?;

        frontend.bind(&args.frontend_address).box_err()?;

        let backend_addr = args.backend_address.clone();
        connect_and_wait(
            &context,
            &backend,
            "proxy-backend",
            |socket| socket.connect(&backend_addr).box_err(),
        )?;

        wait_for_connection(&frontend_monitor)?;

        let handle = Handle::current();
        handle.block_on(start_barrier.wait());

        for _ in 0..args.num_messages {
            let _identity = frontend.recv_msg(0).box_err()?;
            let payload = frontend.recv_msg(0).box_err()?;
            backend.send(payload, 0).box_err()?;
        }

        let handle = Handle::current();
        handle.block_on(end_barrier.wait());

        if args.frontend_address.starts_with("ipc://") {
            let path = args.frontend_address.trim_start_matches("ipc://");
            let _ = std::fs::remove_file(path);
        }

        Ok(())
    })
    .await
    .join_err()
}

fn wait_for_connection(monitor: &zmq::Socket) -> Result<(), BoxError> {
    loop {
        let mut event_msg = zmq::Message::new();
        monitor.recv(&mut event_msg, 0).box_err()?;

        let event_data = &event_msg;
        if event_data.len() >= 2 {
            let event_id = u16::from_le_bytes([event_data[0], event_data[1]]);
            let event = zmq::SocketEvent::from_raw(event_id);
            if event == zmq::SocketEvent::ACCEPTED {
                if monitor.get_rcvmore().box_err()? {
                    let mut _endpoint_msg = zmq::Message::new();
                    monitor.recv(&mut _endpoint_msg, 0).box_err()?;
                }
                return Ok(());
            }
        }

        if monitor.get_rcvmore().box_err()? {
            let mut _endpoint_msg = zmq::Message::new();
            monitor.recv(&mut _endpoint_msg, 0).box_err()?;
        }
    }
}
