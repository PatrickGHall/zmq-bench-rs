use std::sync::Arc;
use tokio::sync::Barrier;
use zmq::Context;

#[derive(Debug, Clone)]
pub struct Args {
    pub payload_size: usize,
    pub num_messages: usize,
    pub router_address: String,
    pub dealer_id: usize,
    pub num_dealers: usize,
    pub benchmark_name: String,
    pub hwm: Option<i32>,
}

fn extract_timestamp(buffer: &[u8]) -> u64 {
    u64::from_le_bytes([
        buffer[0], buffer[1], buffer[2], buffer[3],
        buffer[4], buffer[5], buffer[6], buffer[7],
    ])
}

pub async fn run_async(
    args: Args,
    start_barrier: Arc<Barrier>,
    end_barrier: Arc<Barrier>,
    tsc_per_ns: f64,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let sender_id = args.dealer_id * 2;
    let receiver_id = args.dealer_id * 2 + 1;
    let target_receiver_id = ((args.dealer_id + 1) % args.num_dealers) * 2 + 1;

    let recv_args = args.clone();
    let recv_end_barrier = end_barrier.clone();
    let recv_handle = tokio::task::spawn_blocking(move || -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let context = Context::new();
        let receiver = context.socket(zmq::DEALER).map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
            Box::new(std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
        })?;

        let identity = format!("dealer_{}", receiver_id);
        receiver.set_identity(identity.as_bytes()).map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
            Box::new(std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
        })?;

        if let Some(hwm) = recv_args.hwm {
            receiver.set_rcvhwm(hwm).map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
                Box::new(std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
            })?;
        }

        receiver.connect(&recv_args.router_address).map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
            Box::new(std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
        })?;

        let mut recv_buffer = vec![0u8; recv_args.payload_size];
        let mut latencies = Vec::with_capacity(recv_args.num_messages);

        for _ in 0..recv_args.num_messages {
            receiver.recv_into(&mut recv_buffer, 0).map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
                Box::new(std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
            })?;

            let recv_tsc = unsafe { std::arch::x86_64::_rdtsc() };
            let sent_tsc = extract_timestamp(&recv_buffer);

            latencies.push(recv_tsc - sent_tsc);
        }

        let handle = tokio::runtime::Handle::current();
        handle.block_on(recv_end_barrier.wait());

        use hdrhistogram::Histogram;
        let mut histogram = Histogram::<u64>::new(3).map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
            Box::new(std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
        })?;

        for latency_cycles in latencies {
            let latency_ns = (latency_cycles as f64 / tsc_per_ns) as u64;
            histogram.record(latency_ns).map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
                Box::new(std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
            })?;
        }

        use hdrhistogram::serialization::Serializer;
        use std::fs::File;

        let filename = format!(
            "{}_{}_{}.hgrm",
            recv_args.benchmark_name,
            recv_args.payload_size,
            format!("dealer_{}", recv_args.dealer_id)
        );
        let mut file = File::create(&filename).map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
            Box::new(e)
        })?;
        let mut serializer = hdrhistogram::serialization::V2Serializer::new();
        serializer.serialize(&histogram, &mut file).map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
            Box::new(std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
        })?;

        Ok(())
    });

    let send_args = args.clone();
    let send_barrier = start_barrier.clone();
    let send_handle = tokio::task::spawn_blocking(move || -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let context = Context::new();
        let sender = context.socket(zmq::DEALER).map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
            Box::new(std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
        })?;

        let identity = format!("dealer_{}", sender_id);
        sender.set_identity(identity.as_bytes()).map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
            Box::new(std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
        })?;

        if let Some(hwm) = send_args.hwm {
            sender.set_sndhwm(hwm).map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
                Box::new(std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
            })?;
        }

        let router_addr = send_args.router_address.clone();
        crate::zmq_helpers::connect_and_wait(&context, &sender, &format!("dealer-s-{}", sender_id), |socket| {
            socket.connect(&router_addr).map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
                Box::new(std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
            })
        })?;

        let dest_id = format!("dealer_{}", target_receiver_id).into_bytes();
        let mut send_buffer = vec![0u8; send_args.payload_size];

        let handle = tokio::runtime::Handle::current();
        handle.block_on(send_barrier.wait());

        for _ in 0..send_args.num_messages {
            let send_tsc = unsafe { std::arch::x86_64::_rdtsc() };
            send_buffer[0..8].copy_from_slice(&send_tsc.to_le_bytes());

            sender.send(&dest_id, zmq::SNDMORE).map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
                Box::new(std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
            })?;

            sender.send(&send_buffer, 0).map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
                Box::new(std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
            })?;
        }

        Ok(())
    });

    send_handle.await.map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
        Box::new(std::io::Error::new(
            std::io::ErrorKind::Other,
            format!("Send task join error: {}", e),
        ))
    })??;

    recv_handle.await.map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
        Box::new(std::io::Error::new(
            std::io::ErrorKind::Other,
            format!("Recv task join error: {}", e),
        ))
    })??;

    Ok(())
}
