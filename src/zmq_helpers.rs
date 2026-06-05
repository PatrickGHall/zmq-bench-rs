use std::error::Error;
use std::io::{Error as IoError, ErrorKind};
use tokio::task::JoinError;

pub type BoxError = Box<dyn Error + Send + Sync>;

/// Marker placed in the first 8 bytes of a payload during the hello/probe phase.
/// Receivers use this to know "this is a control/hello message, not a benchmark datum".
pub const HELLO_MARKER: u64 = 0;

pub fn is_hello_marker(buffer: &[u8]) -> bool {
    if buffer.len() < 8 {
        return false;
    }
    let val = u64::from_le_bytes([
        buffer[0], buffer[1], buffer[2], buffer[3], buffer[4], buffer[5], buffer[6], buffer[7],
    ]);
    val == HELLO_MARKER
}

/// Marker sent by sender once (end of synchronization phase) to tell receivers
/// on the data path "synchronization phase is over, the following messages are
/// real benchmark phase data (with TSC timestamps)".
pub const BEGIN_BENCHMARK_MARKER: u64 = u64::MAX;

pub fn is_begin_marker(buffer: &[u8]) -> bool {
    if buffer.len() < 8 {
        return false;
    }
    let val = u64::from_le_bytes([
        buffer[0], buffer[1], buffer[2], buffer[3], buffer[4], buffer[5], buffer[6], buffer[7],
    ]);
    val == BEGIN_BENCHMARK_MARKER
}

pub fn extract_timestamp(buffer: &[u8]) -> u64 {
    u64::from_le_bytes([
        buffer[0], buffer[1], buffer[2], buffer[3], buffer[4], buffer[5], buffer[6], buffer[7],
    ])
}

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

pub trait JoinResultExt<T> {
    fn join_err(self) -> Result<T, BoxError>;
}

impl<T> JoinResultExt<T> for Result<Result<T, BoxError>, JoinError> {
    fn join_err(self) -> Result<T, BoxError> {
        match self {
            Ok(Ok(t)) => Ok(t),
            Ok(Err(e)) => Err(e),
            Err(e) => Err(Box::new(IoError::new(
                ErrorKind::Other,
                format!("Task join error: {}", e),
            ))),
        }
    }
}
