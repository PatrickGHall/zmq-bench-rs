use std::time::Duration;

use zmq::{Context, Socket};

pub fn connect_and_wait<F>(
    context: &Context,
    socket: &Socket,
    monitor_id: &str,
    connect_fn: F,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
where
    F: FnOnce(&Socket) -> Result<(), Box<dyn std::error::Error + Send + Sync>>,
{
    let monitor_endpoint = format!("inproc://monitor-{}", monitor_id);
    socket
        .monitor(&monitor_endpoint, zmq::SocketEvent::CONNECTED as i32)
        .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
            Box::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                e.to_string(),
            ))
        })?;

    let monitor_socket =
        context
            .socket(zmq::PAIR)
            .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
                Box::new(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    e.to_string(),
                ))
            })?;

    monitor_socket.connect(&monitor_endpoint).map_err(
        |e| -> Box<dyn std::error::Error + Send + Sync> {
            Box::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                e.to_string(),
            ))
        },
    )?;

    std::thread::sleep(Duration::from_millis(100));
    connect_fn(socket)?;

    loop {
        let mut event_msg = zmq::Message::new();
        monitor_socket.recv(&mut event_msg, 0).map_err(
            |e| -> Box<dyn std::error::Error + Send + Sync> {
                Box::new(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    e.to_string(),
                ))
            },
        )?;
        let event_data = &event_msg;

        if event_data.len() >= 2 {
            let event_id = u16::from_le_bytes([event_data[0], event_data[1]]);
            let event = zmq::SocketEvent::from_raw(event_id);
            if event == zmq::SocketEvent::CONNECTED {
                if monitor_socket.get_rcvmore().map_err(
                    |e| -> Box<dyn std::error::Error + Send + Sync> {
                        Box::new(std::io::Error::new(
                            std::io::ErrorKind::Other,
                            e.to_string(),
                        ))
                    },
                )? {
                    let mut _endpoint_msg = zmq::Message::new();
                    monitor_socket.recv(&mut _endpoint_msg, 0).map_err(
                        |e| -> Box<dyn std::error::Error + Send + Sync> {
                            Box::new(std::io::Error::new(
                                std::io::ErrorKind::Other,
                                e.to_string(),
                            ))
                        },
                    )?;
                }
                break;
            }
        }
    }

    Ok(())
}
