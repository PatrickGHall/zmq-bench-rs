use crate::zmq_helpers::{
    is_begin_marker, is_hello_marker, maybe_pin_for_bench, maybe_remove_ipc,
    finalize_measurements, hwm, BoxError, JoinResultExt, ZmqResultExt,
    BEGIN_BENCHMARK_MARKER, SyncPhase, compute_hot_loop_timeout,
    make_socket, register_dirty_state, cleanup_dirty_state,
    record_first_hello, record_bench_start, extract_timestamp,
};

#[derive(Clone)]
pub(crate) struct SyncHandles {
    pub first: Arc<AtomicU64>,
    pub last: Arc<AtomicU64>,
}

impl SyncHandles {
    pub fn from_sync(sync: &SyncPhase) -> Self {
        Self {
            first: sync.first_hello_tsc.clone(),
            last: sync.last_bench_start_tsc.clone(),
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) enum Pattern {
    PubSub {
        num_senders: usize,
        num_receivers_per_sender: usize,
    },
    Dealer {
        num_pairs: usize,
    },
    DealerRouter {
        num_dealers: usize,
    },
}

impl Pattern {
    pub(crate) fn name(&self) -> &'static str {
        match self {
            Pattern::PubSub { .. } => "PubSub",
            Pattern::Dealer { .. } => "Dealer",
            Pattern::DealerRouter { .. } => "DealerRouter",
        }
    }

    pub(crate) fn per_item_label(&self) -> &'static str {
        match self {
            Pattern::PubSub { .. } => "pubsub-receiver",
            Pattern::DealerRouter { .. } => "dealerrouter-dealer",
            Pattern::Dealer { .. } => "",
        }
    }

    pub(crate) fn start_message(&self) -> String {
        match self {
            Pattern::PubSub { num_senders, num_receivers_per_sender } => {
                format!("Starting benchmark with {} senders and {} receivers per sender",
                        num_senders, num_receivers_per_sender)
            }
            Pattern::Dealer { num_pairs } => {
                format!("Starting dealer benchmark with {} pairs", num_pairs)
            }
            Pattern::DealerRouter { num_dealers } => {
                format!("Starting dealer-router benchmark with {} dealers", num_dealers)
            }
        }
    }
}
use hdrhistogram::Histogram;
use std::arch::x86_64::_rdtsc;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use tokio::runtime::Handle;
use tokio::sync::{oneshot, Barrier};
use tokio::task::JoinHandle;



pub(crate) async fn run_publisher(
    payload_size: usize,
    num_messages: usize,
    address: String,
    bench_phase: Option<Arc<AtomicBool>>,
    sync: SyncHandles,
    reals_start_rx: Option<oneshot::Receiver<()>>,
) -> Result<(), BoxError> {
    let addr = address.clone();
    let addr_for_remove = addr.clone();
    let sh = sync;
    let n = num_messages;
    let handle = spawn_measurement_sender(
        payload_size,
        move || {
            let p = make_socket(
                zmq::PUB, Some(hwm(num_messages)), None, None,
            )?;
            p.bind(&addr).box_err()?;
            register_dirty_state(&addr);
            Ok(p)
        },
        bench_phase,
        sh,
        reals_start_rx,
        |s, b, _ack| s.send(&b[..], 0).box_err(),
        |s, b, _ack| s.send(&b[..], 0).box_err(),
        move |s, b| hot_send_tsc(s, b, n),
        Some(addr_for_remove),
    );
    handle.await.join_err()
}

pub(crate) async fn run_subscriber(
    addresses: Vec<String>,
    num_messages: usize,
    payload_size: usize,
    hello_ack_tx: Option<oneshot::Sender<()>>,
    begin_ack_tx: Option<oneshot::Sender<()>>,
) -> Result<Histogram<u64>, BoxError> {
    let transport = if addresses.first().map_or(false, |a| a.starts_with("ipc")) { "IPC" } else { "TCP" };
    let addrs = addresses.clone();
    let handle = spawn_measurement_receiver(
        hello_ack_tx,
        begin_ack_tx,
        move || {
            let s = make_socket(
                zmq::SUB, None, Some(hwm(num_messages)), None,
            )?;
            s.set_subscribe(b"").box_err()?;
            for a in &addrs {
                s.connect(a).box_err()?;
            }
            Ok(s)
        },
        None,
        None,
        None,
        "PubSub",
        transport,
        payload_size,
        num_messages,
        true,
    );
    handle.await.join_err()
}



pub(crate) async fn run_sender(
    payload_size: usize,
    num_messages: usize,
    receiver_address: String,
    bench_phase: Option<Arc<AtomicBool>>,
    sync: SyncHandles,
) -> Result<(), BoxError> {
    let addr = receiver_address.clone();
    let sh = sync;
    let n = num_messages;
    let handle = spawn_measurement_sender(
        payload_size,
        move || {
            let hw = hwm(num_messages);
            let d = make_socket(
                zmq::DEALER, Some(hw), Some(hw), None,
            )?;
            d.connect(&addr).box_err()?;
            Ok(d)
        },
        bench_phase,
        sh,
        None,
        |s, b, ack| {
            s.send(&b[..], 0).box_err()?;
            s.recv_into(ack, 0).box_err()?;
            Ok(())
        },
        |s, b, ack| {
            s.send(&b[..], 0).box_err()?;
            s.recv_into(ack, 0).box_err()?;
            Ok(())
        },
        move |s, b| hot_send_tsc(s, b, n),
        None,
    );
    handle.await.join_err()
}

pub(crate) async fn run_receiver(
    payload_size: usize,
    num_messages: usize,
    bind_address: String,
    hello_ack_tx: Option<oneshot::Sender<()>>,
) -> Result<Histogram<u64>, BoxError> {
    let transport = if bind_address.starts_with("ipc") { "IPC" } else { "TCP" };
    let addr = bind_address.clone();
    let addr_for_remove = addr.clone();
    let handle = spawn_measurement_receiver(
        hello_ack_tx,
        None,
        move || {
            let hw = hwm(num_messages);
            let d = make_socket(
                zmq::DEALER, Some(hw), Some(hw), None,
            )?;
            d.bind(&addr).box_err()?;
            register_dirty_state(&addr);
            Ok(d)
        },
        Some(vec![0u8; 8]),
        None,
        Some(addr_for_remove),
        "Dealer",
        transport,
        payload_size,
        num_messages,
        true,
    );
    handle.await.join_err()
}



pub(crate) async fn run_dealer(
    payload_size: usize,
    num_messages: usize,
    router_address: String,
    dealer_id: usize,
    num_dealers: usize,
    hello_ack_tx: oneshot::Sender<()>,
    bench_phase: Option<Arc<AtomicBool>>,
    sync: SyncHandles,
    end_barrier: Arc<Barrier>,
) -> Result<Histogram<u64>, BoxError> {
    let sender_id = dealer_id * 2;
    let receiver_id = dealer_id * 2 + 1;
    let target_receiver_id = ((dealer_id + 1) % num_dealers) * 2 + 1;

    let hello_ack_tx = Some(hello_ack_tx);
    let recv_end_barrier = end_barrier.clone();
    let raddr_for_recv = router_address.clone();
    let recv_id = receiver_id;
    let raddr = raddr_for_recv.clone();
    let recv_handle = spawn_measurement_receiver(
        hello_ack_tx,
        None,
        move || {
            let receiver = make_socket(
                zmq::DEALER,
                None,
                Some(hwm(num_messages)),
                Some(format!("dealer_{}", recv_id).as_bytes()),
            )?;
            receiver.connect(&raddr).box_err()?;
            Ok(receiver)
        },
        None,
        Some(recv_end_barrier),
        None,
        "DealerRouter",
        if raddr_for_recv.starts_with("ipc") { "IPC" } else { "TCP" },
        payload_size,
        num_messages,
        false,
    );

    let first_send = sync.first.clone();
    let last_send = sync.last.clone();

    let dest_id = format!("dealer_{}", target_receiver_id).into_bytes();
    let sh = SyncHandles { first: first_send, last: last_send };
    let n = num_messages;
    let dest = dest_id.clone();
    let send_handle = spawn_measurement_sender(
        payload_size,
        move || {
            let s = make_socket(
                zmq::DEALER, Some(hwm(num_messages)), None, Some(format!("dealer_{}", sender_id).as_bytes()),
            )?;
            s.connect(&router_address).box_err()?;
            Ok(s)
        },
        bench_phase,
        sh,
        None,
        {
            let d = dest.clone();
            move |s, b, _ack| {
                s.send(&d, zmq::SNDMORE).box_err()?;
                s.send(&b[..], 0).box_err()?;
                Ok(())
            }
        },
        {
            let d = dest.clone();
            move |s, b, _ack| {
                s.send(&d, zmq::SNDMORE).box_err()?;
                s.send(&b[..], 0).box_err()?;
                Ok(())
            }
        },
        {
            let d = dest;
            move |s, b| hot_send_tsc_multipart(s, &d, b, n)
        },
        None,
    );

    send_handle.await.join_err()?;
    recv_handle.await.join_err()
}

pub(crate) async fn run_router(
    bind_address: String,
    num_dealers: usize,
    num_messages_per_dealer: usize,
    end_barrier: Arc<Barrier>,
) -> Result<(), BoxError> {
    tokio::task::spawn_blocking(move || -> Result<(), BoxError> {
        maybe_pin_for_bench();
        let total = (num_dealers * num_messages_per_dealer + 1000) as i32;
        let router = make_socket(
            zmq::ROUTER,
            Some(total),
            Some(total),
            None,
        )?;
        router.bind(&bind_address).box_err()?;
        register_dirty_state(&bind_address);

        let total_data = num_dealers * num_messages_per_dealer;
        let mut forwarded = 0usize;

        while forwarded < total_data {
            let _sender = router.recv_msg(0).box_err()?;
            let dest = router.recv_msg(0).box_err()?;
            let payload = router.recv_msg(0).box_err()?;

            let is_control = is_hello_marker(payload.as_ref()) || is_begin_marker(payload.as_ref());
            router.send(dest, zmq::SNDMORE).box_err()?;
            router.send(payload, 0).box_err()?;
            if !is_control { forwarded += 1; }
        }

        Handle::current().block_on(end_barrier.wait());
        maybe_remove_ipc(&bind_address);
        Ok(())
    })
    .await
    .join_err()
}



pub(crate) fn launch_pubsub(
    num_senders: usize,
    num_receivers_per_sender: usize,
    transport: &str,
    payload_size: usize,
    num_messages: usize,
    sync: &SyncPhase,
) -> (Vec<JoinHandle<Result<Histogram<u64>, BoxError>>>,
      Vec<JoinHandle<Result<(), BoxError>>>) {
    let mut hist_tasks = Vec::new();
    let mut completion_tasks = Vec::new();

    for sender_id in 0..num_senders {
        let address = if transport == "ipc" {
            format!("ipc:///tmp/pub_{}.ipc", sender_id)
        } else {
            format!("tcp://127.0.0.1:{}", 5000 + sender_id)
        };

        let mut hello_acks = Vec::new();
        let mut begin_acks = Vec::new();
        for _ in 0..num_receivers_per_sender {
            let (h_tx, h_rx) = oneshot::channel();
            let (b_tx, b_rx) = oneshot::channel();
            hello_acks.push(h_rx);
            begin_acks.push(b_rx);

            let value = address.clone();
            hist_tasks.push(tokio::spawn(async move {
                run_subscriber(vec![value.clone()], num_messages, payload_size, Some(h_tx), Some(b_tx)).await
            }));
        }

        let bench_phase = Arc::new(AtomicBool::new(false));
        let bp = bench_phase.clone();
        let (reals_tx, reals_rx) = oneshot::channel();
        spawn_coordinator(hello_acks, {
            let bp = bp.clone();
            move || { bp.store(true, Ordering::Release); }
        });
        spawn_coordinator(begin_acks, move || {
            let _ = reals_tx.send(());
        });

        let sh = SyncHandles::from_sync(sync);
        completion_tasks.push(tokio::spawn(async move {
            run_publisher(payload_size, num_messages, address.clone(), Some(bench_phase), sh, Some(reals_rx)).await
        }));
    }
    (hist_tasks, completion_tasks)
}

pub(crate) fn launch_dealer(
    num_pairs: usize,
    transport: &str,
    payload_size: usize,
    num_messages: usize,
    sync: &SyncPhase,
) -> (Vec<JoinHandle<Result<Histogram<u64>, BoxError>>>,
      Vec<JoinHandle<Result<(), BoxError>>>) {
    let mut hist_tasks = Vec::new();
    let mut completion_tasks = Vec::new();

    for pair_id in 0..num_pairs {
        let addr = if transport == "ipc" {
            format!("ipc:///tmp/dealer_{}.ipc", pair_id)
        } else {
            format!("tcp://127.0.0.1:{}", 6000 + pair_id)
        };

        let (hello_tx, hello_rx) = oneshot::channel();
        let value = addr.clone();
        hist_tasks.push(tokio::spawn(async move {
            run_receiver(payload_size, num_messages, value.clone(), Some(hello_tx)).await
        }));

        let bp = Arc::new(AtomicBool::new(false));
        let bp2 = bp.clone();
        spawn_coordinator(vec![hello_rx], move || {
            bp2.store(true, Ordering::Release);
        });

        let sh = SyncHandles::from_sync(sync);
        completion_tasks.push(tokio::spawn(async move {
            run_sender(payload_size, num_messages, addr.clone(), Some(bp), sh).await
        }));
    }
    (hist_tasks, completion_tasks)
}

pub(crate) fn launch_dealerrouter(
    num_dealers: usize,
    transport: &str,
    payload_size: usize,
    num_messages: usize,
    sync: &SyncPhase,
) -> (Vec<JoinHandle<Result<Histogram<u64>, BoxError>>>,
      Vec<JoinHandle<Result<(), BoxError>>>) {
    let mut hist_tasks = Vec::new();
    let mut completion_tasks = Vec::new();

    let router_addr = if transport == "ipc" {
        "ipc:///tmp/dealerrouter.ipc".to_string()
    } else {
        "tcp://127.0.0.1:7000".to_string()
    };

    let end_barrier = Arc::new(Barrier::new(num_dealers + 1));
    {
        let b = end_barrier.clone();
        let raddr = router_addr.clone();
        completion_tasks.push(tokio::spawn(async move { run_router(raddr, num_dealers, num_messages, b).await }));
    }

    let mut hello_acks = Vec::new();
    let mut bench_phases = Vec::new();
    for dealer_id in 0..num_dealers {
        let (h_tx, h_rx) = oneshot::channel();
        let bp = Arc::new(AtomicBool::new(false));
        hello_acks.push(h_rx);
        bench_phases.push(bp.clone());

        let sh = SyncHandles::from_sync(sync);
        let b = end_barrier.clone();
        let raddr2 = router_addr.clone();
        hist_tasks.push(tokio::spawn(async move {
            run_dealer(payload_size, num_messages, raddr2, dealer_id, num_dealers, h_tx, Some(bp), sh, b).await
        }));
    }

    spawn_coordinator(hello_acks, move || {
        for p in bench_phases {
            p.store(true, Ordering::Release);
        }
    });

    (hist_tasks, completion_tasks)
}

#[inline]
pub(crate) fn cleanup_ipc(prefix: &str, n: usize) {
    if n > 0 {
        for i in 0..n {
            let _ = std::fs::remove_file(format!("/tmp/{}_{}.ipc", prefix, i));
        }
    } else if prefix == "dealerrouter" {
        let _ = std::fs::remove_file("/tmp/dealerrouter.ipc");
    }
}

fn spawn_coordinator(
    rxs: Vec<oneshot::Receiver<()>>,
    action: impl FnOnce() + Send + 'static,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let timed_out = tokio::time::timeout(std::time::Duration::from_secs(1), async {
            for rx in rxs {
                let _ = rx.await;
            }
        }).await.is_err();
        if timed_out {
            cleanup_dirty_state();
            eprintln!("[FATAL] Likely cause: socket bind/connect ordering, leftover /tmp/*.ipc, pinning/scheduling, or drain not consuming properly.");
            std::process::exit(1);
        }
        action();
    })
}

fn drain_sync_phase(
    socket: &zmq::Socket,
    buffer: &mut [u8],
    mut hello_ack: Option<oneshot::Sender<()>>,
    mut begin_ack: Option<oneshot::Sender<()>>,
    reply_buffer: Option<&[u8]>,
) -> Result<(), BoxError> {
    loop {
        socket.recv_into(buffer, 0).box_err()?;
        if is_hello_marker(buffer) {
            if let Some(tx) = hello_ack.take() {
                let _ = tx.send(());
            }
            if let Some(r) = reply_buffer {
                socket.send(r, 0).box_err()?;
            }
            continue;
        }
        if is_begin_marker(buffer) {
            if let Some(tx) = begin_ack.take() {
                let _ = tx.send(());
            }
            if let Some(r) = reply_buffer {
                socket.send(r, 0).box_err()?;
            }
            break;
        }
        if let Some(r) = reply_buffer {
            socket.send(r, 0).box_err()?;
        }
        continue; // eat reals arriving before BEGIN (ensures clean loop sees exactly N)
    }
    Ok(())
}

const HELLO_DRAIN_RCVTIMEO_MS: i32 = 2000; // short fixed: hello/BEGIN sync phase is independent of bench params (N/payload); longer values masked hangs in prior versions

fn spawn_measurement_receiver(
    hello_ack_tx: Option<oneshot::Sender<()>>,
    begin_ack_tx: Option<oneshot::Sender<()>>,
    make_socket: impl FnOnce() -> Result<zmq::Socket, BoxError> + Send + 'static,
    drain_reply: Option<Vec<u8>>,
    barrier: Option<Arc<Barrier>>,
    remove_addr: Option<String>,
    pattern: &'static str,
    transport: &'static str,
    payload_size: usize,
    num_messages: usize,
    warn_short: bool,
) -> JoinHandle<Result<Histogram<u64>, BoxError>> {
    tokio::spawn(async move {
        tokio::task::spawn_blocking(move || {
            maybe_pin_for_bench();
            let socket = make_socket()?;

            socket.set_rcvtimeo(HELLO_DRAIN_RCVTIMEO_MS).box_err()?;

            let mut recv_buffer = vec![0u8; payload_size];
            let mut latencies = Vec::with_capacity(num_messages);
            let mut recv_cpus: Vec<i32> = Vec::with_capacity(num_messages);

            drain_sync_phase(
                &socket,
                &mut recv_buffer,
                hello_ack_tx,
                begin_ack_tx,
                drain_reply.as_deref(),
            )?;

            let hot_ms = compute_hot_loop_timeout(num_messages, payload_size).as_millis() as i32;
            socket.set_rcvtimeo(hot_ms).box_err()?;

            let received = hot_recv_tsc(&socket, &mut recv_buffer, &mut latencies, &mut recv_cpus, num_messages)?;

            if let Some(b) = barrier {
                Handle::current().block_on(b.wait());
            }

            let (histogram, _) = finalize_measurements(
                latencies, recv_cpus, pattern, transport, payload_size,
            )?;

            if warn_short {
                maybe_warn_short(received, num_messages);
            }

            if let Some(addr) = remove_addr {
                maybe_remove_ipc(&addr);
            }

            Ok(histogram)
        })
        .await
        .join_err()
    })
}

fn spawn_measurement_sender(
    payload_size: usize,
    make_socket: impl FnOnce() -> Result<zmq::Socket, BoxError> + Send + 'static,
    bench_phase: Option<Arc<AtomicBool>>,
    sync: SyncHandles,
    reals_start_rx: Option<oneshot::Receiver<()>>,
    hello_action: impl Fn(&zmq::Socket, &mut [u8], &mut [u8]) -> Result<(), BoxError> + Send + 'static,
    begin_action: impl Fn(&zmq::Socket, &mut [u8], &mut [u8]) -> Result<(), BoxError> + Send + 'static,
    hot_action: impl Fn(&zmq::Socket, &mut [u8]) -> Result<(), BoxError> + Send + 'static,
    remove_addr: Option<String>,
) -> JoinHandle<Result<(), BoxError>> {
    tokio::spawn(async move {
        tokio::task::spawn_blocking(move || {
            maybe_pin_for_bench();
            let socket = make_socket()?;

            let mut buf = vec![0u8; payload_size];
            let mut ack_buf = vec![0u8; 8];
            let handle = Handle::current();

            loop {
                buf[0..8].copy_from_slice(&0u64.to_le_bytes());
                record_first_hello(Some(&sync.first));
                hello_action(&socket, &mut buf, &mut ack_buf)?;
                if let Some(phase) = &bench_phase {
                    if phase.load(Ordering::Acquire) { break; }
                } else { break; }
            }

            buf[0..8].copy_from_slice(&BEGIN_BENCHMARK_MARKER.to_le_bytes());
            begin_action(&socket, &mut buf, &mut ack_buf)?;

            record_bench_start(Some(&sync.last));
            if let Some(rx) = reals_start_rx {
                let _ = handle.block_on(async { rx.await });
            }

            hot_action(&socket, &mut buf)?;

            if let Some(addr) = remove_addr {
                maybe_remove_ipc(&addr);
            }
            Ok(())
        })
        .await
        .join_err()
    })
}

#[inline]
fn maybe_warn_short(received: usize, expected: usize) {
    if received < expected {
        eprintln!(
            "Warning: Received {}/{} messages (dropped {})",
            received,
            expected,
            expected - received
        );
    }
}



#[inline(always)]
fn hot_send_tsc(socket: &zmq::Socket, buf: &mut [u8], n: usize) -> Result<(), BoxError> {
    for _ in 0..n {
        let tsc = unsafe { _rdtsc() };
        buf[0..8].copy_from_slice(&tsc.to_le_bytes());
        socket.send(&buf[..], 0).box_err()?;
    }
    Ok(())
}

#[inline(always)]
fn hot_send_tsc_multipart(socket: &zmq::Socket, dest: &[u8], buf: &mut [u8], n: usize) -> Result<(), BoxError> {
    for _ in 0..n {
        let tsc = unsafe { _rdtsc() };
        buf[0..8].copy_from_slice(&tsc.to_le_bytes());
        socket.send(dest, zmq::SNDMORE).box_err()?;
        socket.send(&buf[..], 0).box_err()?;
    }
    Ok(())
}

#[inline(always)]
fn hot_recv_tsc(
    socket: &zmq::Socket,
    buf: &mut [u8],
    latencies: &mut Vec<u64>,
    cpus: &mut Vec<i32>,
    n: usize,
) -> Result<usize, BoxError> {
    let mut received = 0;
    for _ in 0..n {
        match socket.recv_into(buf, 0) {
            Ok(_) => {
                let recv_tsc = unsafe { _rdtsc() };
                let sent_tsc = extract_timestamp(buf);
                latencies.push(recv_tsc - sent_tsc);
                unsafe { cpus.push(libc::sched_getcpu()); }
                received += 1;
            }
            Err(_) => break,
        }
    }
    Ok(received)
}
