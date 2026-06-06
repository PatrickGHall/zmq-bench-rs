use crate::zmq_helpers::{
    is_begin_marker, is_hello_marker, maybe_pin_for_bench, maybe_remove_ipc,
    finalize_measurements, hwm, BoxError, JoinResultExt, ZmqResultExt,
    BEGIN_BENCHMARK_MARKER, HELLO_MARKER, SyncPhase, compute_hot_loop_timeout,
    make_socket, register_dirty_state, cleanup_dirty_state,
    record_first_hello, record_bench_start, read_leading_u64, context, ThroughputSample,
    get_tsc_per_ns,
};

/// Build a ZMQ endpoint for the chosen transport: an IPC socket file under /tmp
/// or a loopback TCP port.
fn endpoint(transport: &str, ipc_name: &str, tcp_port: u16) -> String {
    if transport == "ipc" {
        format!("ipc:///tmp/{}.ipc", ipc_name)
    } else {
        format!("tcp://127.0.0.1:{}", tcp_port)
    }
}

/// The CSV/label transport name ("IPC"/"TCP") for an endpoint address.
fn transport_label(addr: &str) -> &'static str {
    if addr.starts_with("ipc") { "IPC" } else { "TCP" }
}

/// Wait on the end barrier with an upper bound. The barrier rendezvous (router
/// finished forwarding <-> dealers finished receiving) completes in microseconds
/// on a healthy run; the timeout is a safety net so a partner that died without
/// arriving turns into a clean teardown rather than a permanent hang.
const END_BARRIER_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);

fn end_barrier_wait_bounded(b: &Barrier) {
    Handle::current().block_on(async {
        if tokio::time::timeout(END_BARRIER_TIMEOUT, b.wait()).await.is_err() {
            eprintln!("warning: end barrier timed out after {:?} (partner did not arrive)", END_BARRIER_TIMEOUT);
        }
    });
}

/// A task producing one receiver's latency histogram and throughput sample.
type HistTask = JoinHandle<Result<(Histogram<u64>, ThroughputSample), BoxError>>;
/// A task that only needs to run to completion (sender / router).
type CompletionTask = JoinHandle<Result<(), BoxError>>;
/// Tasks launched for one benchmark: histogram producers and completion-only tasks.
type LaunchedTasks = (Vec<HistTask>, Vec<CompletionTask>);

/// Sends one received message back to its origin, used only in latency mode to
/// lock-step the sender (keeping the queue empty so latency reflects transit).
type EchoFn = Box<dyn Fn(&zmq::Socket, &[u8]) -> Result<(), BoxError> + Send>;

/// What a benchmark measures.
///
///   * `Throughput` — sender blasts all N messages; the deep send queue means the
///     recorded latency is dominated by queue residence (Little's law), so this
///     mode is about messages/second.
///   * `Latency` — the offered load is held low so the queue stays empty and the
///     recorded latency is true one-way transit. Dealer/DealerRouter use a
///     lock-step ping-pong (sender waits for an echo); PUB/SUB has no back channel
///     so its sender is rate-paced instead.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum Mode {
    Throughput,
    Latency,
}

impl Mode {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Mode::Throughput => "throughput",
            Mode::Latency => "latency",
        }
    }
}

impl std::str::FromStr for Mode {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, String> {
        match s.to_ascii_lowercase().as_str() {
            "throughput" | "tput" => Ok(Mode::Throughput),
            "latency" | "lat" => Ok(Mode::Latency),
            other => Err(format!("unknown mode '{}' (expected throughput|latency)", other)),
        }
    }
}

/// Offered-load spacing for latency-mode PUB/SUB: one send per interval keeps the
/// subscriber's queue empty (it drains far faster) so latency reflects transit,
/// not queueing. Lock-step patterns don't need this — the echo paces them.
const LATENCY_PACE_NS: u64 = 25_000;

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
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::runtime::Handle;
use tokio::sync::{oneshot, Barrier};
use tokio::task::JoinHandle;



pub(crate) async fn run_publisher(
    payload_size: usize,
    num_messages: usize,
    address: String,
    bench_phase: Option<Arc<AtomicBool>>,
    sync: SyncPhase,
    reals_start_rx: Option<oneshot::Receiver<()>>,
    mode: Mode,
) -> Result<(), BoxError> {
    let addr = address.clone();
    let addr_for_remove = addr.clone();
    let n = num_messages;
    let pace_ticks = (LATENCY_PACE_NS as f64 * get_tsc_per_ns()) as u64;
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
        sync,
        reals_start_rx,
        |s, b, _ack| s.send(&b[..], 0).box_err(),
        // PUB/SUB has no back channel, so latency mode rate-paces the sender
        // rather than ping-ponging.
        move |s, b| match mode {
            Mode::Throughput => hot_send_tsc(s, b, n),
            Mode::Latency => hot_send_paced(s, b, n, pace_ticks),
        },
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
) -> Result<(Histogram<u64>, ThroughputSample), BoxError> {
    let transport = addresses.first().map(|a| transport_label(a)).unwrap_or("TCP");
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
        None,
    );
    handle.await.join_err()
}



pub(crate) async fn run_sender(
    payload_size: usize,
    num_messages: usize,
    receiver_address: String,
    bench_phase: Option<Arc<AtomicBool>>,
    sync: SyncPhase,
    mode: Mode,
) -> Result<(), BoxError> {
    let addr = receiver_address.clone();
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
        sync,
        None,
        |s, b, ack| {
            s.send(&b[..], 0).box_err()?;
            s.recv_into(ack, 0).box_err()?;
            Ok(())
        },
        move |s, b| match mode {
            Mode::Throughput => hot_send_tsc(s, b, n),
            Mode::Latency => hot_send_pingpong(s, None, b, n),
        },
        None,
    );
    handle.await.join_err()
}

pub(crate) async fn run_receiver(
    payload_size: usize,
    num_messages: usize,
    bind_address: String,
    hello_ack_tx: Option<oneshot::Sender<()>>,
    mode: Mode,
) -> Result<(Histogram<u64>, ThroughputSample), BoxError> {
    let transport = transport_label(&bind_address);
    let addr = bind_address.clone();
    let addr_for_remove = addr.clone();
    // Latency mode: echo each message straight back to the paired sender (direct
    // DEALER link, no identity frame) so it can release the next one.
    let echo: Option<EchoFn> = match mode {
        Mode::Throughput => None,
        Mode::Latency => Some(Box::new(|s: &zmq::Socket, buf: &[u8]| s.send(buf, 0).box_err())),
    };
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
        echo,
    );
    handle.await.join_err()
}



#[allow(clippy::too_many_arguments)] // wires up two sockets + sync/barrier handles for one ring node
pub(crate) async fn run_dealer(
    payload_size: usize,
    num_messages: usize,
    router_address: String,
    dealer_id: usize,
    num_dealers: usize,
    hello_ack_tx: oneshot::Sender<()>,
    bench_phase: Option<Arc<AtomicBool>>,
    sync: SyncPhase,
    end_barrier: Arc<Barrier>,
    mode: Mode,
) -> Result<(Histogram<u64>, ThroughputSample), BoxError> {
    // Each dealer owns a sender (id 2k) and a receiver (id 2k+1) and forwards to
    // the next dealer's receiver, forming a ring through the router.
    let sender_id = dealer_id * 2;
    let receiver_id = dealer_id * 2 + 1;
    let target_receiver_id = ((dealer_id + 1) % num_dealers) * 2 + 1;
    // This receiver is fed by the previous ring node's sender; latency mode
    // echoes back to it (through the router) to lock-step that sender.
    let predecessor_sender_id = ((dealer_id + num_dealers - 1) % num_dealers) * 2;
    let transport = transport_label(&router_address);

    let recv_addr = router_address.clone();
    let echo: Option<EchoFn> = match mode {
        Mode::Throughput => None,
        Mode::Latency => {
            let back = format!("dealer_{}", predecessor_sender_id).into_bytes();
            Some(Box::new(move |s: &zmq::Socket, buf: &[u8]| {
                s.send(&back, zmq::SNDMORE).box_err()?;
                s.send(buf, 0).box_err()
            }))
        }
    };
    let recv_handle = spawn_measurement_receiver(
        Some(hello_ack_tx),
        None,
        move || {
            let receiver = make_socket(
                zmq::DEALER, None, Some(hwm(num_messages)),
                Some(format!("dealer_{}", receiver_id).as_bytes()),
            )?;
            receiver.connect(&recv_addr).box_err()?;
            Ok(receiver)
        },
        None,
        Some(end_barrier),
        None,
        "DealerRouter",
        transport,
        payload_size,
        num_messages,
        true,
        echo,
    );

    let dest = format!("dealer_{}", target_receiver_id).into_bytes();
    let n = num_messages;
    let send_handle = spawn_measurement_sender(
        payload_size,
        move || {
            let s = make_socket(
                zmq::DEALER, Some(hwm(num_messages)), None,
                Some(format!("dealer_{}", sender_id).as_bytes()),
            )?;
            s.connect(&router_address).box_err()?;
            Ok(s)
        },
        bench_phase,
        sync,
        None,
        {
            let d = dest.clone();
            move |s, b, _ack| {
                s.send(&d, zmq::SNDMORE).box_err()?;
                s.send(&b[..], 0).box_err()?;
                Ok(())
            }
        },
        move |s, b| match mode {
            Mode::Throughput => hot_send_tsc_multipart(s, &dest, b, n),
            Mode::Latency => hot_send_pingpong(s, Some(&dest), b, n),
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
    mode: Mode,
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
        // Mandatory delivery turns the default lossy ROUTER behaviour into
        // explicit EAGAIN (peer pipe full) / EHOSTUNREACH (peer not yet
        // connected) errors, so the forward loop can apply backpressure instead
        // of silently dropping measured messages.
        router.set_router_mandatory(true).box_err()?;
        router.bind(&bind_address).box_err()?;
        register_dirty_state(&bind_address);

        // Latency mode adds a return hop (each forward is echoed back to its
        // sender), so the router relays twice as many real messages.
        let relays_per_message = if mode == Mode::Latency { 2 } else { 1 };
        let expected = num_dealers * num_messages_per_dealer * relays_per_message;
        let mut forwarded = 0usize;

        while forwarded < expected {
            let _sender = router.recv_msg(0).box_err()?;
            let dest = router.recv_msg(0).box_err()?;
            let payload = router.recv_msg(0).box_err()?;

            let is_control = is_hello_marker(payload.as_ref()) || is_begin_marker(payload.as_ref());
            router_forward_reliable(&router, &dest, &payload)?;
            if !is_control { forwarded += 1; }
        }
        end_barrier_wait_bounded(&end_barrier);
        maybe_remove_ipc(&bind_address);
        Ok(())
    })
    .await
    .join_err()
}

/// Deadline for the router to keep retrying a forward before declaring the path
/// dead. Healthy forwards succeed immediately (or after a sub-ms connection
/// window); this only bounds a genuinely dead peer so it errors instead of
/// hanging. Kept short so failures surface fast rather than masking a problem.
const ROUTER_FORWARD_DEADLINE: std::time::Duration = std::time::Duration::from_secs(2);

/// Forward `[dest, payload]` through a ROUTER set to mandatory mode, retrying
/// transient conditions instead of dropping. The routing/HWM decision is taken
/// on the first (identity) frame, so we retry there: once `dest` is accepted the
/// message is committed to that peer's pipe and the payload frame follows.
///
///   * EAGAIN       — peer pipe at HWM; spin until the receiver drains (backpressure).
///   * EHOSTUNREACH — peer not connected yet (startup race); spin until it appears.
///
/// A bounded deadline turns a genuinely dead peer into a clear error rather than
/// an unbounded hang.
fn router_forward_reliable(router: &zmq::Socket, dest: &zmq::Message, payload: &zmq::Message)
    -> Result<(), BoxError>
{
    let start = std::time::Instant::now();
    loop {
        match router.send(&dest[..], zmq::SNDMORE) {
            Ok(()) => {
                router.send(&payload[..], 0).box_err()?;
                return Ok(());
            }
            Err(zmq::Error::EAGAIN) | Err(zmq::Error::EHOSTUNREACH)
                if start.elapsed() < ROUTER_FORWARD_DEADLINE =>
            {
                std::thread::yield_now();
            }
            Err(e) => return Err(Box::new(std::io::Error::other(
                format!("router forward failed (peer unreachable for {:?}): {}",
                        start.elapsed(), e),
            ))),
        }
    }
}



pub(crate) fn launch_pubsub(
    num_senders: usize,
    num_receivers_per_sender: usize,
    transport: &str,
    payload_size: usize,
    num_messages: usize,
    sync: &SyncPhase,
    mode: Mode,
) -> LaunchedTasks {
    let mut hist_tasks = Vec::new();
    let mut completion_tasks = Vec::new();

    for sender_id in 0..num_senders {
        let address = endpoint(transport, &format!("pub_{}", sender_id), (5000 + sender_id) as u16);

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

        let sh = sync.clone();
        completion_tasks.push(tokio::spawn(async move {
            run_publisher(payload_size, num_messages, address.clone(), Some(bench_phase), sh, Some(reals_rx), mode).await
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
    mode: Mode,
) -> LaunchedTasks {
    let mut hist_tasks = Vec::new();
    let mut completion_tasks = Vec::new();

    for pair_id in 0..num_pairs {
        let addr = endpoint(transport, &format!("dealer_{}", pair_id), (6000 + pair_id) as u16);

        let (hello_tx, hello_rx) = oneshot::channel();
        let value = addr.clone();
        hist_tasks.push(tokio::spawn(async move {
            run_receiver(payload_size, num_messages, value.clone(), Some(hello_tx), mode).await
        }));

        let bp = Arc::new(AtomicBool::new(false));
        let bp2 = bp.clone();
        spawn_coordinator(vec![hello_rx], move || {
            bp2.store(true, Ordering::Release);
        });

        let sh = sync.clone();
        completion_tasks.push(tokio::spawn(async move {
            run_sender(payload_size, num_messages, addr.clone(), Some(bp), sh, mode).await
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
    mode: Mode,
) -> LaunchedTasks {
    let mut hist_tasks = Vec::new();
    let mut completion_tasks = Vec::new();

    let router_addr = endpoint(transport, "dealerrouter", 7000);

    let end_barrier = Arc::new(Barrier::new(num_dealers + 1));
    {
        let b = end_barrier.clone();
        let raddr = router_addr.clone();
        completion_tasks.push(tokio::spawn(async move { run_router(raddr, num_dealers, num_messages, b, mode).await }));
    }

    let mut hello_acks = Vec::new();
    let mut bench_phases = Vec::new();
    for dealer_id in 0..num_dealers {
        let (h_tx, h_rx) = oneshot::channel();
        let bp = Arc::new(AtomicBool::new(false));
        hello_acks.push(h_rx);
        bench_phases.push(bp.clone());

        let sh = sync.clone();
        let b = end_barrier.clone();
        let raddr2 = router_addr.clone();
        hist_tasks.push(tokio::spawn(async move {
            run_dealer(payload_size, num_messages, raddr2, dealer_id, num_dealers, h_tx, Some(bp), sh, b, mode).await
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

/// How long the coordinator waits for every receiver's HELLO ack before
/// declaring the handshake broken. The phase is sub-millisecond when healthy, so
/// this only bounds a genuine setup failure.
const SYNC_ACK_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(1);

fn spawn_coordinator(
    rxs: Vec<oneshot::Receiver<()>>,
    action: impl FnOnce() + Send + 'static,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let timed_out = tokio::time::timeout(SYNC_ACK_TIMEOUT, async {
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
    let mut eagain_retries = 0;
    loop {
        match socket.recv_into(buffer, 0) {
            Ok(_) => {}
            // A slow partner (still binding/connecting/pinning) yields EAGAIN after
            // the short rcvtimeo. Retry up to a bounded budget rather than treating
            // the first 2s gap as fatal — the previous code crashed (~25% of runs)
            // here under startup contention. A genuine hang still surfaces quickly.
            Err(zmq::Error::EAGAIN) if eagain_retries < HELLO_DRAIN_MAX_EAGAIN => {
                eagain_retries += 1;
                continue;
            }
            Err(zmq::Error::EAGAIN) => {
                return Err(Box::new(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    format!(
                        "sync handshake stalled: no marker after {} x {}ms",
                        HELLO_DRAIN_MAX_EAGAIN + 1, HELLO_DRAIN_RCVTIMEO_MS
                    ),
                )));
            }
            Err(e) => return Err(Box::new(std::io::Error::other(e.to_string()))),
        }
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

const HELLO_RESEND_INTERVAL: std::time::Duration = std::time::Duration::from_millis(1); // throttle sync-phase HELLO resends (non-measured phase)
const HELLO_DRAIN_RCVTIMEO_MS: i32 = 1000; // sync phase is sub-ms healthy; one expired window already signals a setup problem
const HELLO_DRAIN_MAX_EAGAIN: u32 = 1; // tolerate a single slow window, then fail fast (~2s total) for quick debugging

#[allow(clippy::too_many_arguments)] // generic receiver harness: handshake channels, socket builder, and reporting metadata
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
    echo: Option<EchoFn>,
) -> JoinHandle<Result<(Histogram<u64>, ThroughputSample), BoxError>> {
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

            let (received, span_tsc) = hot_recv_tsc(
                &socket, &mut recv_buffer, &mut latencies, &mut recv_cpus,
                num_messages, context().save_hists, echo.as_ref(),
            )?;

            if let Some(b) = barrier {
                end_barrier_wait_bounded(&b);
            }

            let (histogram, sample) = finalize_measurements(
                latencies, recv_cpus, span_tsc, pattern, transport, payload_size,
            )?;

            if warn_short {
                maybe_warn_short(received, num_messages);
            }

            if let Some(addr) = remove_addr {
                maybe_remove_ipc(&addr);
            }

            Ok((histogram, sample))
        })
        .await
        .join_err()
    })
}

#[allow(clippy::too_many_arguments)] // generic sender harness: socket builder, sync handles, and the two send closures
fn spawn_measurement_sender(
    payload_size: usize,
    make_socket: impl FnOnce() -> Result<zmq::Socket, BoxError> + Send + 'static,
    bench_phase: Option<Arc<AtomicBool>>,
    sync: SyncPhase,
    reals_start_rx: Option<oneshot::Receiver<()>>,
    // Same action drives both sync markers (HELLO and BEGIN): send the framed
    // buffer and, for request/reply patterns, consume the ack.
    sync_action: impl Fn(&zmq::Socket, &mut [u8], &mut [u8]) -> Result<(), BoxError> + Send + 'static,
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

            // Synchronization phase: resend HELLO until the coordinator flips
            // bench_phase (every receiver has seen a HELLO and acked). Repetition
            // is required because PUB/SUB drops messages sent before the subscriber
            // finishes connecting (slow-joiner). Throttled to once per millisecond
            // so the resend can't flood the send-HWM ahead of bench_phase (an
            // unthrottled loop pushed tens of thousands of HELLOs and stalled
            // forwarding); a healthy handshake completes in a handful of HELLOs.
            buf[0..8].copy_from_slice(&HELLO_MARKER.to_le_bytes());
            loop {
                record_first_hello(&sync.first_hello_tsc);
                sync_action(&socket, &mut buf, &mut ack_buf)?;
                match &bench_phase {
                    Some(phase) if !phase.load(Ordering::Acquire) => {
                        std::thread::sleep(HELLO_RESEND_INTERVAL);
                    }
                    _ => break,
                }
            }

            buf[0..8].copy_from_slice(&BEGIN_BENCHMARK_MARKER.to_le_bytes());
            sync_action(&socket, &mut buf, &mut ack_buf)?;

            record_bench_start(&sync.last_bench_start_tsc);
            if let Some(rx) = reals_start_rx {
                let _ = handle.block_on(rx);
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
        stamp_tsc(buf);
        socket.send(&buf[..], 0).box_err()?;
    }
    Ok(())
}

#[inline(always)]
fn hot_send_tsc_multipart(socket: &zmq::Socket, dest: &[u8], buf: &mut [u8], n: usize) -> Result<(), BoxError> {
    for _ in 0..n {
        stamp_tsc(buf);
        socket.send(dest, zmq::SNDMORE).box_err()?;
        socket.send(&buf[..], 0).box_err()?;
    }
    Ok(())
}

#[inline(always)]
fn stamp_tsc(buf: &mut [u8]) {
    buf[0..8].copy_from_slice(&unsafe { _rdtsc() }.to_le_bytes());
}

/// Latency-mode PUB/SUB sender: stamp + send, then busy-wait `pace_ticks` so the
/// offered load stays well below the subscriber's drain rate (empty queue).
#[inline(always)]
fn hot_send_paced(socket: &zmq::Socket, buf: &mut [u8], n: usize, pace_ticks: u64) -> Result<(), BoxError> {
    for _ in 0..n {
        stamp_tsc(buf);
        socket.send(&buf[..], 0).box_err()?;
        let start = unsafe { _rdtsc() };
        while unsafe { _rdtsc() } - start < pace_ticks {
            std::hint::spin_loop();
        }
    }
    Ok(())
}

/// Latency-mode lock-step sender: send one stamped message and block until the
/// receiver's echo returns before sending the next. Only one message is ever in
/// flight, so the receiver records pure transit latency. `dest` is `Some` for the
/// DEALER/ROUTER (identity-routed) hop and `None` for the direct DEALER pair.
#[inline(always)]
fn hot_send_pingpong(socket: &zmq::Socket, dest: Option<&[u8]>, buf: &mut [u8], n: usize) -> Result<(), BoxError> {
    let mut echo = vec![0u8; buf.len()];
    for _ in 0..n {
        stamp_tsc(buf);
        if let Some(d) = dest {
            socket.send(d, zmq::SNDMORE).box_err()?;
        }
        socket.send(&buf[..], 0).box_err()?;
        socket.recv_into(&mut echo, 0).box_err()?;
    }
    Ok(())
}

/// Consecutive idle `recvtimeo` windows tolerated before declaring the stream
/// stalled. The reliable router never drops, so the receiver should always reach
/// `n` with messages flowing continuously; a transient scheduling gap merely
/// yields one empty window and we retry. Only a genuine hang produces this many
/// back-to-back empty windows.
const HOT_RECV_MAX_IDLE_WINDOWS: u32 = 2;

/// Receive `n` messages, recording each one's latency. Returns the count
/// received and the TSC span from the first to the last receive (for throughput).
///
/// The hot loop is kept minimal so the receiver keeps pace with the sender: any
/// asymmetric per-message work here lets the send-side queue grow and inflates
/// measured latency. sched_getcpu() is therefore only sampled when `collect_cpus`
/// (i.e. --save-hists) needs it for core-correlation.
#[inline(always)]
fn hot_recv_tsc(
    socket: &zmq::Socket,
    buf: &mut [u8],
    latencies: &mut Vec<u64>,
    cpus: &mut Vec<i32>,
    n: usize,
    collect_cpus: bool,
    echo: Option<&EchoFn>,
) -> Result<(usize, u64), BoxError> {
    let mut received = 0;
    let mut idle = 0u32;
    let (mut first_tsc, mut last_tsc) = (0u64, 0u64);
    // A recvtimeo expiry (EAGAIN) is a transient gap, not end-of-stream — retry
    // until `n` arrive or too many empty windows in a row (true stall). Breaking
    // on the first gap used to abandon the stream and deadlock the end barrier.
    while received < n {
        match socket.recv_into(buf, 0) {
            Ok(_) => {
                let recv_tsc = unsafe { _rdtsc() };
                if received == 0 { first_tsc = recv_tsc; }
                last_tsc = recv_tsc;
                latencies.push(recv_tsc - read_leading_u64(buf));
                if collect_cpus {
                    unsafe { cpus.push(libc::sched_getcpu()); }
                }
                // Latency mode: bounce the message back so the sender (which is
                // blocked waiting for it) can release the next one.
                if let Some(echo) = echo {
                    echo(socket, buf)?;
                }
                received += 1;
                idle = 0;
            }
            Err(zmq::Error::EAGAIN) => {
                idle += 1;
                if idle >= HOT_RECV_MAX_IDLE_WINDOWS {
                    break;
                }
            }
            Err(_) => break,
        }
    }
    Ok((received, last_tsc.saturating_sub(first_tsc)))
}
