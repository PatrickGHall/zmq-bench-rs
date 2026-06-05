use std::error::Error;
use std::io::{Error as IoError, ErrorKind};
use tokio::task::JoinError;

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
