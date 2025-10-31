use crate::zmq_helpers::{BoxError, HdrResultExt, JoinResultExt, ZmqResultExt};
use hdrhistogram::serialization::Serializer;
use hdrhistogram::Histogram;
use std::arch::x86_64::_rdtsc;
use std::fs::File;
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Handle;
use tokio::sync::Barrier;
use tokio::time::sleep;
use zmq::Context;

#[derive(Debug, Clone)]
pub struct Args {
    pub payload_size: usize,
    pub num_messages: usize,
    pub hwm: i32,
    pub batch_sleep_ms: u64,
    pub router_address: String,
    pub dealer_id: usize,
    pub num_dealers: usize,
    pub benchmark_name: String,
}

fn extract_timestamp(buffer: &[u8]) -> u64 {
    u64::from_le_bytes([
        buffer[0], buffer[1], buffer[2], buffer[3], buffer[4], buffer[5], buffer[6], buffer[7],
    ])
}

pub async fn run_async(
    args: Args,
    end_barrier: Arc<Barrier>,
    tsc_per_ns: f64,
) -> Result<(), BoxError> {
    let sender_id = args.dealer_id * 2;
    let receiver_id = args.dealer_id * 2 + 1;
    let target_receiver_id = ((args.dealer_id + 1) % args.num_dealers) * 2 + 1;

    let recv_args = args.clone();
    let recv_end_barrier = end_barrier.clone();
    let recv_handle = tokio::task::spawn_blocking(move || -> Result<(), BoxError> {
        let context = Context::new();
        let receiver = context.socket(zmq::DEALER).box_err()?;

        let identity = format!("dealer_{}", receiver_id);
        receiver.set_identity(identity.as_bytes()).box_err()?;
        receiver.set_rcvhwm(args.hwm).box_err()?;

        receiver.connect(&recv_args.router_address).box_err()?;

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
            format!("dealer_{}", recv_args.dealer_id)
        );
        let mut file = File::create(&filename)?;
        let mut serializer = hdrhistogram::serialization::V2Serializer::new();
        serializer.serialize(&histogram, &mut file).box_err()?;

        Ok(())
    });

    let send_args = args.clone();
    let send_handle = tokio::task::spawn_blocking(move || -> Result<(), BoxError> {
        let context = Context::new();
        let sender = context.socket(zmq::DEALER).box_err()?;

        let identity = format!("dealer_{}", sender_id);
        sender.set_identity(identity.as_bytes()).box_err()?;
        sender.set_sndhwm(args.hwm).box_err()?;

        let router_addr = send_args.router_address.clone();
        sender.connect(&router_addr).box_err()?;

        let dest_id = format!("dealer_{}", target_receiver_id).into_bytes();
        let mut send_buffer = vec![0u8; send_args.payload_size];

        let handle = Handle::current();
        handle.block_on(sleep(Duration::from_millis(500)));

        let batch_size = args.hwm as usize;

        for i in 0..send_args.num_messages {
            let send_tsc = unsafe { _rdtsc() };
            send_buffer[0..8].copy_from_slice(&send_tsc.to_le_bytes());

            sender.send(&dest_id, zmq::SNDMORE).box_err()?;
            sender.send(&send_buffer, 0).box_err()?;

            if send_args.batch_sleep_ms > 0 && (i + 1) % batch_size == 0 {
                handle.block_on(sleep(Duration::from_millis(args.batch_sleep_ms)));
            }
        }

        Ok(())
    });

    send_handle.await.join_err()?;
    recv_handle.await.join_err()?;

    Ok(())
}
