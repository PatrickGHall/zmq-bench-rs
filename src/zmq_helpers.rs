use hdrhistogram::Histogram;
use hdrhistogram::serialization::Serializer;
use serde::Serialize;
use std::error::Error;
use std::fs;
use std::io::Error as IoError;
use std::path::Path;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tokio::task::JoinError;

pub type BoxError = Box<dyn Error + Send + Sync>;

/// First message of the handshake: subscriber/dealer is alive but not yet ready.
pub const HELLO_MARKER: u64 = 0;
/// Last handshake message: real (timestamped) traffic follows immediately after.
pub const BEGIN_BENCHMARK_MARKER: u64 = u64::MAX;

/// Read the leading little-endian u64 from a message. Every message's first 8
/// bytes are either a marker or the send-time TSC; callers guarantee len >= 8.
#[inline]
pub fn read_leading_u64(buffer: &[u8]) -> u64 {
    u64::from_le_bytes(buffer[..8].try_into().unwrap())
}

pub fn is_hello_marker(buffer: &[u8]) -> bool {
    buffer.len() >= 8 && read_leading_u64(buffer) == HELLO_MARKER
}

pub fn is_begin_marker(buffer: &[u8]) -> bool {
    buffer.len() >= 8 && read_leading_u64(buffer) == BEGIN_BENCHMARK_MARKER
}

pub trait ZmqResultExt<T> {
    fn box_err(self) -> Result<T, BoxError>;
}

impl<T> ZmqResultExt<T> for Result<T, zmq::Error> {
    fn box_err(self) -> Result<T, BoxError> {
        self.map_err(|e| -> BoxError { Box::new(IoError::other(e.to_string())) })
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
            Err(e) => Err(Box::new(IoError::other(format!("Task join error: {}", e)))),
        }
    }
}

/// Unified global static context for the entire program run.
/// All program-wide configuration, computed values, and mutable runtime state
/// live here so we avoid scattering OnceLock/Mutex/Atomic statics and reduce
/// parameter passing for things that are truly global to one invocation of the tool.
#[derive(Debug)]
pub struct Context {
    // --- CLI / program-wide arguments (set once at startup) ---
    pub save_hists: bool,
    pub output: String,

    // --- Computed values (initialized on first use) ---
    tsc_per_ns: OnceLock<f64>,

    // --- Mutable runtime state (thread-safe) ---
    save_counter: AtomicUsize,
    next_cpu: AtomicUsize,
    /// Physical CPU count captured once at startup, before any thread pins
    /// itself. Used as the fixed round-robin modulus in assign_next_cpu.
    total_cpus: usize,
    dirty_state: std::sync::Mutex<Vec<String>>,
}

static CONTEXT: OnceLock<Context> = OnceLock::new();

/// Initialize the global context from CLI args. Must be called early in main
/// before any benchmark work. Subsequent calls are ignored.
pub fn init_context(save_hists: bool, output: String) {
    let ctx = Context {
        save_hists,
        output,
        tsc_per_ns: OnceLock::new(),
        save_counter: AtomicUsize::new(0),
        next_cpu: AtomicUsize::new(0),
        total_cpus: num_cpus::get().max(1),
        dirty_state: std::sync::Mutex::new(Vec::new()),
    };
    let _ = CONTEXT.set(ctx);
}

/// Retrieve the global context. Panics with a clear message if not yet initialized.
pub fn context() -> &'static Context {
    CONTEXT.get().expect("Global Context not initialized; call init_context() early in main")
}



/// Returns the calibrated TSC ticks per nanosecond. The (somewhat expensive)
/// calibration is performed on first call and the result is cached globally
/// inside the Context.
pub fn get_tsc_per_ns() -> f64 {
    context().get_tsc_per_ns()
}

impl Context {
    /// Returns the calibrated TSC ticks per nanosecond (lazy, cached).
    pub fn get_tsc_per_ns(&self) -> f64 {
        *self.tsc_per_ns.get_or_init(|| {
            println!("Calibrating TSC...");

            if self.save_hists {
                log_affinity_info("main_calibrate_start");
            }

            // 5 x 2ms = 10ms total. A 2ms window still spans ~8M TSC ticks, so
            // the ticks/ns ratio is precise to well under 0.1%; taking the median
            // rejects the occasional sample perturbed by a scheduling hiccup.
            const CALIBRATION_SAMPLES: usize = 5;
            const CALIBRATION_SLEEP: std::time::Duration = std::time::Duration::from_millis(2);
            let mut tsc_per_ns_samples = Vec::with_capacity(CALIBRATION_SAMPLES);

            for _ in 0..CALIBRATION_SAMPLES {
                let time_start = std::time::Instant::now();
                let tsc_start = unsafe { std::arch::x86_64::_rdtsc() };

                std::thread::sleep(CALIBRATION_SLEEP);
                let tsc_end = unsafe { std::arch::x86_64::_rdtsc() };
                let elapsed_ns = time_start.elapsed().as_nanos() as u64;

                let tsc_per_ns = (tsc_end - tsc_start) as f64 / elapsed_ns as f64;
                tsc_per_ns_samples.push(tsc_per_ns);
            }

            tsc_per_ns_samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
            let val = tsc_per_ns_samples[CALIBRATION_SAMPLES / 2];

            println!(
                "TSC calibration: {:.3} GHz ({:.6} cycles/ns)",
                val, val
            );

            val
        })
    }

    pub fn next_save_id(&self) -> usize {
        self.save_counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    }

    /// Round-robin a distinct CPU for the calling thread.
    ///
    /// Uses `total_cpus` captured once at startup rather than a live
    /// `num_cpus::get()`: the latter is affinity-aware, so once any thread pins
    /// itself to a single core that 1-CPU mask is inherited by child threads and
    /// `num_cpus::get()` would return 1 — collapsing every later assignment onto
    /// core 0 (`raw % 1`). The fixed modulus keeps assignments spread across all
    /// physical cores.
    pub fn assign_next_cpu(&self) -> usize {
        self.next_cpu.fetch_add(1, Ordering::Relaxed) % self.total_cpus
    }

    /// Track an endpoint so its backing file can be removed on early exit. Only
    /// `ipc://` leaves a filesystem artifact; `tcp://` needs no cleanup.
    pub fn register_dirty_state(&self, addr: &str) {
        if addr.starts_with("ipc://") {
            if let Ok(mut guard) = self.dirty_state.lock() {
                guard.push(addr.to_string());
            }
        }
    }

    pub fn cleanup_dirty_state(&self) {
        let items = match self.dirty_state.lock() {
            Ok(mut guard) => std::mem::take(&mut *guard),
            Err(_) => return,
        };
        for addr in items {
            maybe_remove_ipc(&addr);
        }
    }
}

pub fn next_save_id() -> usize {
    context().next_save_id()
}

pub fn save_histogram_hgrm(prefix: &str, hist: &Histogram<u64>) -> Result<(), BoxError> {
    let filename = format!("{}.hgrm", prefix);
    let file = std::fs::File::create(&filename)?;
    let mut serializer = hdrhistogram::serialization::V2Serializer::new();
    serializer.serialize(hist, &mut std::io::BufWriter::new(file))?;
    Ok(())
}

pub fn save_raw_latencies(prefix: &str, latencies_ns: &[u64]) -> Result<(), BoxError> {
    let filename = format!("{}.rawlat.txt", prefix);
    let mut f = std::fs::File::create(&filename)?;
    for &l in latencies_ns {
        std::io::Write::write_all(&mut f, format!("{}\n", l).as_bytes())?;
    }
    Ok(())
}

pub fn assign_next_cpu() -> usize {
    context().assign_next_cpu()
}

pub fn pin_current_thread_to_cpu(cpu: usize) -> Result<(), BoxError> {
    unsafe {
        let mut cpuset: libc::cpu_set_t = std::mem::zeroed();
        libc::CPU_ZERO(&mut cpuset);
        libc::CPU_SET(cpu, &mut cpuset);
        let ret = libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &cpuset);
        if ret != 0 {
            eprintln!(
                "warning: sched_setaffinity to cpu {} failed: {} (continuing unpinned)",
                cpu,
                std::io::Error::last_os_error()
            );
        } else if context().save_hists {
            let current = libc::sched_getcpu();
            eprintln!("pinned current thread to cpu {} (getcpu={})", cpu, current);
            log_affinity_info("pin_success");
        }
    }
    Ok(())
}

pub fn maybe_pin_for_bench() {
    if context().save_hists {
        log_affinity_info("pre_pin");
    }
    let cpu = assign_next_cpu();
    let _ = pin_current_thread_to_cpu(cpu);
}

pub fn log_affinity_info(label: &str) {
    if !context().save_hists {
        return;
    }
    unsafe {
        let cpu = libc::sched_getcpu();
        eprintln!("[AFFINITY-INFO] {} current_cpu={} (use with SAVE_HISTS raw data to correlate lats vs core/CCD)", label, cpu);
    }
}

/// Shared timestamps spanning the synchronization phase: the first HELLO sent by
/// any sender and the last entry into the measured phase. Cloning shares the same
/// atomics, so every sender updates the one instance the main task later prints.
#[derive(Clone)]
pub struct SyncPhase {
    pub first_hello_tsc: Arc<AtomicU64>,
    pub last_bench_start_tsc: Arc<AtomicU64>,
}

impl SyncPhase {
    pub fn new() -> Self {
        Self {
            first_hello_tsc: Arc::new(AtomicU64::new(u64::MAX)),
            last_bench_start_tsc: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn print(&self, tsc_per_ns: f64) {
        let first = self.first_hello_tsc.load(Ordering::Relaxed);
        let last = self.last_bench_start_tsc.load(Ordering::Relaxed);
        if last > first && first != u64::MAX {
            let ns = ((last - first) as f64 / tsc_per_ns) as u64;
            println!(
                "Synchronization phase duration: {} ns ({:.3} ms)",
                ns,
                ns as f64 / 1_000_000.0
            );
        }
    }
}

/// Earliest moment any sender began the handshake (min across senders).
#[inline]
pub fn record_first_hello(first: &AtomicU64) {
    first.fetch_min(unsafe { std::arch::x86_64::_rdtsc() }, Ordering::Relaxed);
}

/// Latest moment any sender entered the measured phase (max across senders).
#[inline]
pub fn record_bench_start(last: &AtomicU64) {
    last.fetch_max(unsafe { std::arch::x86_64::_rdtsc() }, Ordering::Relaxed);
}

pub fn collect_and_append_result(
    pattern: &str,
    transport: &str,
    payload_size_bytes: usize,
    histograms: Vec<Histogram<u64>>,
    throughput: Vec<ThroughputSample>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut total = Histogram::<u64>::new(3)?;
    for h in &histograms {
        total.add(h)?;
    }

    // Receivers run concurrently, so aggregate system throughput is the total
    // messages drained over the longest receive span (the others overlap it).
    let received: usize = throughput.iter().map(|t| t.received).sum();
    let span_ns = throughput.iter().map(|t| t.span_ns).max().unwrap_or(0);
    let msgs_per_sec = if span_ns > 0 { received as f64 * 1e9 / span_ns as f64 } else { 0.0 };
    let mb_per_sec = msgs_per_sec * payload_size_bytes as f64 / 1e6;

    append_benchmark_result(pattern, transport, payload_size_bytes, &total, msgs_per_sec, mb_per_sec)
}

pub fn print_per_item_stats(label: &str, histograms: &[Histogram<u64>]) {
    if histograms.len() > 1 {
        for (i, h) in histograms.iter().enumerate() {
            println!(
                "  {label}[{i}] p50={} p99={}",
                h.value_at_percentile(50.0),
                h.value_at_percentile(99.0)
            );
        }
    }
}

pub fn maybe_save_individual_hist(
    pattern: &str,
    transport: &str,
    payload_size: usize,
    histogram: &Histogram<u64>,
    latencies_ns: &[u64],
    recv_cpus: &[i32],
) {
    if context().save_hists {
        let save_id = next_save_id();
        let prefix = format!(
            "indiv_{}_{}_{}_{}_{}",
            pattern,
            transport,
            payload_size,
            std::process::id(),
            save_id
        );
        let _ = save_raw_latencies(&prefix, latencies_ns);
        let _ = save_histogram_hgrm(&prefix, histogram);
        let cpus_path = format!("{}.recv_cpus.txt", prefix);
        let cpus_str: String = recv_cpus.iter().map(|c| format!("{}\n", c)).collect();
        let _ = std::fs::write(cpus_path, cpus_str);
    }
}



#[derive(Debug, Serialize)]
struct BenchmarkResult {
    pattern: String,
    transport: String,
    payload_size_bytes: usize,
    min_latency_ns: u64,
    median_latency_ns: u64,
    p99_latency_ns: u64,
    max_latency_ns: u64,
    msgs_per_sec: u64,
    mb_per_sec: f64,
}

pub fn append_benchmark_result(
    pattern: &str,
    transport: &str,
    payload_size_bytes: usize,
    histogram: &Histogram<u64>,
    msgs_per_sec: f64,
    mb_per_sec: f64,
) -> Result<(), Box<dyn std::error::Error>> {
    let result = BenchmarkResult {
        pattern: pattern.to_string(),
        transport: transport.to_string(),
        payload_size_bytes,
        min_latency_ns: histogram.min(),
        median_latency_ns: histogram.value_at_percentile(50.0),
        p99_latency_ns: histogram.value_at_percentile(99.0),
        max_latency_ns: histogram.max(),
        msgs_per_sec: msgs_per_sec as u64,
        mb_per_sec: (mb_per_sec * 100.0).round() / 100.0,
    };

    let csv_path = &context().output;
    let file_exists = Path::new(csv_path).exists();

    let mut writer = csv::WriterBuilder::new()
        .has_headers(!file_exists)
        .from_writer(fs::OpenOptions::new().append(true).create(true).open(csv_path)?);

    writer.serialize(result)?;
    writer.flush()?;

    println!("Aggregated benchmark result for {}-{} ({} bytes)", pattern, transport, payload_size_bytes);
    println!(
        "  stats: min={} p50={} mean={:.0} p90={} p99={} p99.9={} max={}",
        histogram.min(),
        histogram.value_at_percentile(50.0),
        histogram.mean(),
        histogram.value_at_percentile(90.0),
        histogram.value_at_percentile(99.0),
        histogram.value_at_percentile(99.9),
        histogram.max()
    );
    println!("  throughput: {:.0} msg/s ({:.1} MB/s)", msgs_per_sec, mb_per_sec);

    Ok(())
}

#[inline]
pub fn hwm(n: usize) -> i32 {
    (n as i32) + 1000
}

/// End-to-end receive throughput for one measurement stream: how many messages
/// were drained and the wall-clock span from the first to the last received.
#[derive(Clone, Copy)]
pub struct ThroughputSample {
    pub received: usize,
    pub span_ns: u64,
}

pub fn finalize_measurements(
    latencies: Vec<u64>,
    recv_cpus: Vec<i32>,
    recv_span_tsc: u64,
    pattern: &str,
    transport: &str,
    payload_size: usize,
) -> Result<(Histogram<u64>, ThroughputSample), BoxError> {
    let received = latencies.len();
    let tsc_per_ns = get_tsc_per_ns();
    let latencies_ns: Vec<u64> = latencies
        .iter()
        .map(|&c| (c as f64 / tsc_per_ns) as u64)
        .collect();

    let mut histogram = Histogram::<u64>::new(3)?;
    for &latency_ns in &latencies_ns {
        histogram.record(latency_ns)?;
    }

    maybe_save_individual_hist(pattern, transport, payload_size, &histogram, &latencies_ns, &recv_cpus);

    let span_ns = (recv_span_tsc as f64 / tsc_per_ns) as u64;
    Ok((histogram, ThroughputSample { received, span_ns }))
}

#[inline]
pub fn maybe_remove_ipc(addr: &str) {
    if addr.starts_with("ipc://") {
        let path = addr.trim_start_matches("ipc://");
        let _ = std::fs::remove_file(path);
    }
}


pub fn make_socket(
    typ: zmq::SocketType,
    snd_hwm: Option<i32>,
    rcv_hwm: Option<i32>,
    identity: Option<&[u8]>,
) -> Result<zmq::Socket, BoxError> {
    let ctx = ::zmq::Context::new();
    let s = ctx.socket(typ).box_err()?;
    if let Some(h) = snd_hwm {
        s.set_sndhwm(h).box_err()?;
    }
    if let Some(h) = rcv_hwm {
        s.set_rcvhwm(h).box_err()?;
    }
    if let Some(id) = identity {
        s.set_identity(id).box_err()?;
    }
    Ok(s)
}



pub fn register_dirty_state(addr: &str) {
    context().register_dirty_state(addr);
}

pub fn cleanup_dirty_state() {
    context().cleanup_dirty_state();
}

/// Per-receive timeout for the measured phase, scaled to the data volume:
/// estimate the time to push all messages (a fixed per-message cost plus a
/// per-byte term), double it for headroom, and floor at 1s so a healthy
/// localhost run never times out on scheduling jitter. A stalled stream then
/// fails within a small multiple of this rather than hanging.
pub fn compute_hot_loop_timeout(num_messages: usize, payload_size: usize) -> Duration {
    let per_msg_ns = 5_000u64 + payload_size as u64 * 20;
    let estimate_ns = (num_messages as u64).saturating_mul(per_msg_ns);
    Duration::from_nanos(estimate_ns.saturating_mul(2).max(1_000_000_000))
}
