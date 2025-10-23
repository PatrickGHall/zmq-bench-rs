use std::error::Error;
use std::io::{Error as IoError, ErrorKind};
use tokio::runtime::Handle;
use tokio::task::JoinError;
use tokio::time::{sleep, Duration};
use zmq::{Context, Socket};

pub type BoxError = Box<dyn Error + Send + Sync>;

pub trait ZmqResultExt<T> {
    fn box_err(self) -> Result<T, BoxError>;
}

impl<T> ZmqResultExt<T> for Result<T, zmq::Error> {
    fn box_err(self) -> Result<T, BoxError> {
        self.map_err(|e| -> BoxError { Box::new(IoError::new(ErrorKind::Other, e.to_string())) })
    }
}

pub trait HdrResultExt<T> {
    fn box_err(self) -> Result<T, BoxError>;
}

impl<T> HdrResultExt<T> for Result<T, hdrhistogram::CreationError> {
    fn box_err(self) -> Result<T, BoxError> {
        self.map_err(|e| -> BoxError { Box::new(e) })
    }
}

impl<T> HdrResultExt<T> for Result<T, hdrhistogram::RecordError> {
    fn box_err(self) -> Result<T, BoxError> {
        self.map_err(|e| -> BoxError { Box::new(e) })
    }
}

impl<T> HdrResultExt<T> for Result<T, hdrhistogram::serialization::V2SerializeError> {
    fn box_err(self) -> Result<T, BoxError> {
        self.map_err(|e| -> BoxError { Box::new(e) })
    }
}

pub trait JoinResultExt {
    fn join_err(self) -> Result<(), BoxError>;
}

impl JoinResultExt for Result<Result<(), BoxError>, JoinError> {
    fn join_err(self) -> Result<(), BoxError> {
        match self {
            Ok(Ok(())) => Ok(()),
            Ok(Err(e)) => Err(e),
            Err(e) => Err(Box::new(IoError::new(
                ErrorKind::Other,
                format!("Task join error: {}", e),
            ))),
        }
    }
}

pub fn connect_and_wait<F>(
    context: &Context,
    socket: &Socket,
    monitor_id: &str,
    connect_fn: F,
) -> Result<(), BoxError>
where
    F: FnOnce(&Socket) -> Result<(), BoxError>,
{
    use ZmqResultExt;

    let monitor_endpoint = format!("inproc://monitor-{}", monitor_id);
    socket
        .monitor(&monitor_endpoint, zmq::SocketEvent::CONNECTED as i32)
        .box_err()?;

    let monitor_socket = context.socket(zmq::PAIR).box_err()?;
    monitor_socket.connect(&monitor_endpoint).box_err()?;

    let handle = Handle::current();
    handle.block_on(sleep(Duration::from_millis(100)));
    connect_fn(socket)?;

    loop {
        let mut event_msg = zmq::Message::new();
        monitor_socket.recv(&mut event_msg, 0).box_err()?;
        let event_data = &event_msg;

        if event_data.len() >= 2 {
            let event_id = u16::from_le_bytes([event_data[0], event_data[1]]);
            let event = zmq::SocketEvent::from_raw(event_id);
            if event == zmq::SocketEvent::CONNECTED {
                if monitor_socket.get_rcvmore().box_err()? {
                    let mut _endpoint_msg = zmq::Message::new();
                    monitor_socket.recv(&mut _endpoint_msg, 0).box_err()?;
                }
                break;
            }
        }
    }

    Ok(())
}
