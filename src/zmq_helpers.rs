use hdrhistogram::Histogram;
use hdrhistogram::serialization::Serializer;
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

use std::sync::atomic::{AtomicUsize, Ordering};

static SAVE_COUNTER: AtomicUsize = AtomicUsize::new(0);

static NEXT_CPU: AtomicUsize = AtomicUsize::new(0);

pub fn next_save_id() -> usize {
    SAVE_COUNTER.fetch_add(1, Ordering::Relaxed)
}

/// Temporarily save a histogram to .hgrm (binary V2) for later inspection.
pub fn save_histogram_hgrm(prefix: &str, hist: &Histogram<u64>) -> Result<(), BoxError> {
    let filename = format!("{}.hgrm", prefix);
    let file = std::fs::File::create(&filename)?;
    let mut serializer = hdrhistogram::serialization::V2Serializer::new();
    serializer.serialize(hist, &mut std::io::BufWriter::new(file)).box_err()?;
    Ok(())
}

/// Temporarily save raw per-message latencies (ns) as newline-separated text for easy Python/numpy analysis.
pub fn save_raw_latencies(prefix: &str, latencies_ns: &[u64]) -> Result<(), BoxError> {
    let filename = format!("{}.rawlat.txt", prefix);
    let mut f = std::fs::File::create(&filename)?;
    for &l in latencies_ns {
        std::io::Write::write_all(&mut f, format!("{}\n", l).as_bytes())?;
    }
    Ok(())
}

/// Return the next CPU id (round-robin over available cores). Used for pinning.
pub fn assign_next_cpu() -> usize {
    let n = num_cpus::get().max(1);
    NEXT_CPU.fetch_add(1, Ordering::Relaxed) % n
}

/// Pin the *current OS thread* to a specific CPU (linux only; no-op elsewhere).
/// Non-fatal on failure (just prints warning). Called unconditionally (unless
/// ZMQ_BENCH_NO_AFFINITY=1) from all measurement and send paths.
pub fn pin_current_thread_to_cpu(cpu: usize) -> Result<(), BoxError> {
    #[cfg(target_os = "linux")]
    {
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
            } else if std::env::var("ZMQ_BENCH_SAVE_HISTS").is_ok() {
                // When user is saving histograms for analysis/digging, surface the actual
                // core assignments so they can correlate with any remaining variance.
                // Also getcpu to confirm.
                let current = libc::sched_getcpu();
                eprintln!("pinned current thread to cpu {} (getcpu={})", cpu, current);
                log_affinity_info("pin_success");
            }
        }
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = cpu;
    }
    Ok(())
}

/// CPU affinity pinning for benchmark threads (now enabled by default).
/// Pins measurement (and related) threads round-robin to reduce noise, imbalance,
/// and extreme tails from thread migration / preemption on different cores.
/// Call this at the start of hot blocking tasks.
/// Can be disabled with ZMQ_BENCH_NO_AFFINITY=1 if needed for debugging.
///
/// For Threadripper (multi-CCD): externally restrict allowed CPUs with taskset
/// to cores within one or more CCDs (see `lscpu -e`, /proc/cpuinfo physical/core ids,
/// or `numactl --hardware` to identify CCD-local cores for cache locality).
/// Internal pinning will then round-robin only within your externally allowed set.
/// Use SAVE_HISTS=1 to log pinned cpu + getcpu() for verification that external+internal
/// affinity is as expected (no cross-CCD surprises).
pub fn maybe_pin_for_bench() {
    if std::env::var("ZMQ_BENCH_SAVE_HISTS").is_ok() {
        log_affinity_info("pre_pin");
    }
    if std::env::var("ZMQ_BENCH_NO_AFFINITY").is_err() {
        let cpu = assign_next_cpu();
        let _ = pin_current_thread_to_cpu(cpu);
    }
}

/// Log detailed current affinity info (for the calling thread) when SAVE_HISTS=1.
/// Useful for confirming pinning (internal + any external taskset) and correlating
/// with latency anomalies in post-analysis. Call early in measurement threads.
pub fn log_affinity_info(context: &str) {
    if std::env::var("ZMQ_BENCH_SAVE_HISTS").is_ok() {
        #[cfg(target_os = "linux")]
        unsafe {
            let cpu = libc::sched_getcpu();
            eprintln!("[AFFINITY-INFO] {} current_cpu={} (use with SAVE_HISTS raw data to correlate lats vs core/CCD)", context, cpu);
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = context;
        }
    }
}
