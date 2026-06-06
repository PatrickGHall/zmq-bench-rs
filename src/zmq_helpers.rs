use csv;
use hdrhistogram::Histogram;
use hdrhistogram::serialization::Serializer;
use serde::Serialize;
use std::error::Error;
use std::fs;
use std::io::{Error as IoError, ErrorKind};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinError;

pub type BoxError = Box<dyn Error + Send + Sync>;

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

use std::sync::atomic::AtomicUsize;
use std::sync::OnceLock;

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

            const CALIBRATION_SAMPLES: usize = 10;
            let mut tsc_per_ns_samples = Vec::with_capacity(CALIBRATION_SAMPLES);

            for _ in 0..CALIBRATION_SAMPLES {
                let tsc_start = unsafe { std::arch::x86_64::_rdtsc() };
                let time_start = std::time::SystemTime::now();

                std::thread::sleep(std::time::Duration::from_millis(10));
                let tsc_end = unsafe { std::arch::x86_64::_rdtsc() };
                let time_end = std::time::SystemTime::now();

                let elapsed_ns = time_end.duration_since(time_start).unwrap().as_nanos() as u64;
                let elapsed_tsc = tsc_end - tsc_start;
                let tsc_per_ns = elapsed_tsc as f64 / elapsed_ns as f64;
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

    pub fn register_dirty_state(&self, addr: &str) {
        if addr.starts_with("ipc://") {
            if let Ok(mut guard) = self.dirty_state.lock() {
                guard.push(addr.to_string());
            }
        } else if addr.starts_with("tcp://") {
            if let Ok(mut guard) = self.dirty_state.lock() {
                guard.push(addr.to_string());
            }
        }
    }

    pub fn cleanup_dirty_state(&self) {
        let mut items = Vec::new();
        if let Ok(mut guard) = self.dirty_state.lock() {
            items = std::mem::take(&mut *guard);
        }
        for addr in items {
            if addr.starts_with("ipc://") {
                let path = addr.trim_start_matches("ipc://");
                let _ = std::fs::remove_file(path);
            }
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
    serializer.serialize(hist, &mut std::io::BufWriter::new(file)).box_err()?;
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
            let delta_tsc = last - first;
            let ns = (delta_tsc as f64 / tsc_per_ns) as u64;
            println!(
                "Synchronization phase duration: {} ns ({:.3} ms)",
                ns,
                ns as f64 / 1_000_000.0
            );
        }
    }

}

#[inline]
pub fn record_first_hello(first: Option<&Arc<AtomicU64>>) {
    if let Some(f) = first {
        let t = unsafe { std::arch::x86_64::_rdtsc() };
        f.fetch_min(t, Ordering::Relaxed);
    }
}

#[inline]
pub fn record_bench_start(last: Option<&Arc<AtomicU64>>) {
    if let Some(l) = last {
        let t = unsafe { std::arch::x86_64::_rdtsc() };
        l.fetch_max(t, Ordering::Relaxed);
    }
}

pub fn collect_and_append_result(
    pattern: &str,
    transport: &str,
    payload_size_bytes: usize,
    histograms: Vec<Histogram<u64>>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut total = Histogram::<u64>::new(3)?;
    for h in &histograms {
        total.add(h)?;
    }

    append_benchmark_result(pattern, transport, payload_size_bytes, &total)
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
}

pub fn append_benchmark_result(
    pattern: &str,
    transport: &str,
    payload_size_bytes: usize,
    histogram: &Histogram<u64>,
) -> Result<(), Box<dyn std::error::Error>> {
    let result = BenchmarkResult {
        pattern: pattern.to_string(),
        transport: transport.to_string(),
        payload_size_bytes,
        min_latency_ns: histogram.min(),
        median_latency_ns: histogram.value_at_percentile(50.0),
        p99_latency_ns: histogram.value_at_percentile(99.0),
        max_latency_ns: histogram.max(),
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

    Ok(())
}

#[inline]
pub fn hwm(n: usize) -> i32 {
    (n as i32) + 1000
}

pub fn finalize_measurements(
    latencies: Vec<u64>,
    recv_cpus: Vec<i32>,
    pattern: &str,
    transport: &str,
    payload_size: usize,
) -> Result<(Histogram<u64>, usize), BoxError> {
    let received = latencies.len();
    let tsc_per_ns = get_tsc_per_ns();
    let latencies_ns: Vec<u64> = latencies
        .iter()
        .map(|&c| (c as f64 / tsc_per_ns) as u64)
        .collect();

    let mut histogram = Histogram::<u64>::new(3).box_err()?;
    for &latency_ns in &latencies_ns {
        histogram.record(latency_ns).box_err()?;
    }

    maybe_save_individual_hist(pattern, transport, payload_size, &histogram, &latencies_ns, &recv_cpus);

    Ok((histogram, received))
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

/// Hot loop rcvtimeo computed from data volume the suite pushes.
/// X is estimated push time for (N msgs * (payload + overhead)); timeout = X*2.
/// Suite completes very fast on local ZMQ, so this yields low-second timeouts
/// (much tighter than any fixed 30s/60s). Only non-standard policy is the *2
/// safety factor + volume basis (names alone don't communicate the calc).
pub fn compute_hot_loop_timeout(num_messages: usize, payload_size: usize) -> Duration {
    // Per-iter cost model (base + payload) * N gives X; *2 per spec.
    // Floor at 1s so normal localhost runs (even with jitter/pinning) don't spuriously timeout.
    let per_iter_ns = 5_000u64 + (payload_size as u64 * 20);
    let x_ns = (num_messages as u64).saturating_mul(per_iter_ns);
    Duration::from_nanos(x_ns.saturating_mul(2).max(1_000_000_000))
}
