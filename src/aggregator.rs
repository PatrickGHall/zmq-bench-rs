use hdrhistogram::Histogram;
use serde::Serialize;
use std::fs;
use std::path::Path;

#[derive(Debug, Serialize)]
struct BenchmarkResult {
    pattern: String,
    transport: String,
    payload_size_bytes: usize,
    min_latency_ns: u64,
    median_latency_ns: u64,
    max_latency_ns: u64,
}

/// Append a (pooled) histogram result as one row to results.csv.
/// This replaces the old external `aggregator` subcommand + .hgrm file scanning.
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
        max_latency_ns: histogram.max(),
    };

    let csv_path = "results.csv";
    let file_exists = Path::new(csv_path).exists();

    let mut writer = csv::WriterBuilder::new()
        .has_headers(!file_exists)
        .from_writer(fs::OpenOptions::new().append(true).create(true).open(csv_path)?);

    writer.serialize(result)?;
    writer.flush()?;

    println!("Aggregated benchmark result for {}-{} ({} bytes)", pattern, transport, payload_size_bytes);

    Ok(())
}