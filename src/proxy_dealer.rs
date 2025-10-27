use crate::zmq_helpers::{connect_and_wait, BoxError, HdrResultExt, JoinResultExt, ZmqResultExt};
use hdrhistogram::serialization::Serializer;
use hdrhistogram::Histogram;
use std::arch::x86_64::_rdtsc;
use std::fs::File;
use std::sync::Arc;
use tokio::runtime::Handle;
use tokio::sync::Barrier;
use zmq::Context;

#[derive(Debug, Clone)]
pub struct Args {
    pub frontend_address: String,
    pub backend_address: String,
    pub payload_size: usize,
    pub num_messages: usize,
    pub pair_id: usize,
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
) -> Result<(), BoxError> {
    let recv_args = args.clone();
    let recv_end_barrier = end_barrier.clone();
    let recv_handle = tokio::task::spawn_blocking(move || -> Result<(), BoxError> {
        let context = Context::new();
        let receiver = context.socket(zmq::DEALER).box_err()?;

        if let Some(hwm) = recv_args.hwm {
            receiver.set_rcvhwm(hwm).box_err()?;
        }

        receiver.bind(&recv_args.backend_address).box_err()?;

        let mut recv_buffer = vec![0u8; recv_args.payload_size];
        let mut latencies = Vec::with_capacity(recv_args.num_messages);

        for _ in 0..recv_args.num_messages {
            receiver.recv_into(&mut recv_buffer, 0).box_err()?;

            let recv_tsc = unsafe { _rdtsc() };
            let sent_tsc = extract_timestamp(&recv_buffer);

            latencies.push(recv_tsc - sent_tsc);
        }

        let handle = Handle::current();
        handle.block_on(recv_end_barrier.wait());

        let mut histogram = Histogram::<u64>::new(3).box_err()?;

        for latency_cycles in latencies {
            let latency_ns = (latency_cycles as f64 / tsc_per_ns) as u64;
            histogram.record(latency_ns).box_err()?;
        }

        let filename = format!(
            "{}_{}_{}.hgrm",
            recv_args.benchmark_name,
            recv_args.payload_size,
            format!("pair_{}", recv_args.pair_id)
        );
        let mut file = File::create(&filename)?;
        let mut serializer = hdrhistogram::serialization::V2Serializer::new();
        serializer.serialize(&histogram, &mut file).box_err()?;

        if recv_args.backend_address.starts_with("ipc://") {
            let path = recv_args.backend_address.trim_start_matches("ipc://");
            let _ = std::fs::remove_file(path);
        }

        Ok(())
    });

    let send_args = args.clone();
    let send_barrier = start_barrier.clone();
    let send_handle = tokio::task::spawn_blocking(move || -> Result<(), BoxError> {
        let context = Context::new();
        let sender = context.socket(zmq::DEALER).box_err()?;

        if let Some(hwm) = send_args.hwm {
            sender.set_sndhwm(hwm).box_err()?;
        }

        let frontend_addr = send_args.frontend_address.clone();
        connect_and_wait(
            &context,
            &sender,
            &format!("proxy-sender-{}", send_args.pair_id),
            |socket| socket.connect(&frontend_addr).box_err(),
        )?;

        let mut send_buffer = vec![0u8; send_args.payload_size];

        let handle = Handle::current();
        handle.block_on(send_barrier.wait());

        for _ in 0..send_args.num_messages {
            let send_tsc = unsafe { _rdtsc() };
            send_buffer[0..8].copy_from_slice(&send_tsc.to_le_bytes());

            sender.send(&send_buffer, 0).box_err()?;
        }

        Ok(())
    });

    send_handle.await.join_err()?;
    recv_handle.await.join_err()?;

    Ok(())
}
