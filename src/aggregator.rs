use clap::Parser;
use hdrhistogram::Histogram;
use serde::Serialize;
use std::fs;
use std::io::Read;
use std::path::Path;

#[derive(Parser, Debug)]
pub struct Args {
    #[clap(long)]
    pub pattern: String,
    #[clap(long)]
    pub transport: String,
    #[clap(long)]
    pub payload_size: usize,
}

#[derive(Debug, Serialize)]
struct BenchmarkResult {
    pattern: String,
    transport: String,
    payload_size_bytes: usize,
    min_latency_ns: u64,
    median_latency_ns: u64,
    max_latency_ns: u64,
}

pub fn run(args: Args) -> Result<(), Box<dyn std::error::Error>> {
    let mut total_histogram = Histogram::<u64>::new(3)?;
    let mut deserializer = hdrhistogram::serialization::Deserializer::new();
    let mut count = 0;

    for entry in fs::read_dir(".")? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().map_or(false, |e| e == "hgrm") {
            let mut file = fs::File::open(&path)?;
            let mut buffer = Vec::new();
            file.read_to_end(&mut buffer)?;
            let histogram = deserializer.deserialize(&mut &buffer[..])?;
            total_histogram.add(histogram)?;
            fs::remove_file(path)?;
            count += 1;
        }
    }

    if count == 0 {
        eprintln!("Warning: No histogram files found");
        return Ok(());
    }

    let result = BenchmarkResult {
        pattern: args.pattern,
        transport: args.transport,
        payload_size_bytes: args.payload_size,
        min_latency_ns: total_histogram.min(),
        median_latency_ns: total_histogram.value_at_percentile(50.0),
        max_latency_ns: total_histogram.max(),
    };

    let csv_path = "results.csv";
    let file_exists = Path::new(csv_path).exists();

    let mut writer = csv::WriterBuilder::new()
        .has_headers(!file_exists)
        .from_writer(fs::OpenOptions::new().append(true).create(true).open(csv_path)?);

    writer.serialize(result)?;
    writer.flush()?;

    println!("Aggregated {} histogram files", count);

    Ok(())
}