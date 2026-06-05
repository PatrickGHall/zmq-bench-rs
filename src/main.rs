use clap::Parser;
use hdrhistogram::Histogram;
use std::arch::x86_64::_rdtsc;
use std::error::Error;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::runtime::Builder;
use tokio::sync::Barrier;
mod aggregator;
mod dealer_receiver;
mod dealer_sender;
mod dealerrouter_dealer;
mod dealerrouter_router;
mod publisher;
mod subscriber;
mod zmq_helpers;

fn calibrate_tsc() -> f64 {
    const CALIBRATION_SAMPLES: usize = 10;

    let mut tsc_per_ns_samples = Vec::with_capacity(CALIBRATION_SAMPLES);

    for _ in 0..CALIBRATION_SAMPLES {
        let tsc_start = unsafe { _rdtsc() };
        let time_start = SystemTime::now();

        std::thread::sleep(Duration::from_millis(10));
        let tsc_end = unsafe { _rdtsc() };
        let time_end = SystemTime::now();

        let elapsed_ns = time_end.duration_since(time_start).unwrap().as_nanos() as u64;
        let elapsed_tsc = tsc_end - tsc_start;
        let tsc_per_ns = elapsed_tsc as f64 / elapsed_ns as f64;
        tsc_per_ns_samples.push(tsc_per_ns);
    }

    tsc_per_ns_samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
    tsc_per_ns_samples[CALIBRATION_SAMPLES / 2]
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[command(subcommand)]
    command: Command,
}

#[derive(Parser, Debug)]
enum Command {
    PubsubBenchmark(PubsubBenchmarkArgs),
    DealerBenchmark(DealerBenchmarkArgs),
    DealerRouterBenchmark(DealerRouterBenchmarkArgs),
    RunBenchmarks(RunBenchmarksArgs),
}

#[derive(Parser, Debug)]
struct PubsubBenchmarkArgs {
    #[clap(long, default_value = "1")]
    pub num_senders: usize,

    #[clap(long, default_value = "1")]
    pub num_receivers_per_sender: usize,

    #[clap(long, default_value = "10000")]
    pub num_messages: usize,

    #[clap(long, default_value = "64")]
    pub payload_size: usize,

    #[clap(long, default_value = "ipc")]
    pub transport: String,

    #[clap(long, default_value = "1000")]
    pub hwm: i32,

    #[clap(long, default_value = "10")]
    pub batch_sleep_ms: u64,
}

#[derive(Parser, Debug)]
struct DealerBenchmarkArgs {
    #[clap(long, default_value = "1")]
    pub num_pairs: usize,

    #[clap(long, default_value = "10000")]
    pub num_messages: usize,

    #[clap(long, default_value = "64")]
    pub payload_size: usize,

    #[clap(long, default_value = "ipc")]
    pub transport: String,
}

#[derive(Parser, Debug)]
struct DealerRouterBenchmarkArgs {
    #[clap(long, default_value = "1")]
    pub num_dealers: usize,

    #[clap(long, default_value = "10000")]
    pub num_messages: usize,

    #[clap(long, default_value = "64")]
    pub payload_size: usize,

    #[clap(long, default_value = "ipc")]
    pub transport: String,

    #[clap(long, default_value = "1000")]
    pub hwm: i32,

    #[clap(long, default_value = "10")]
    pub batch_sleep_ms: u64,
}

/// Full suite runner. Args and defaults match the original run_benchmarks.sh script.
#[derive(Parser, Debug)]
struct RunBenchmarksArgs {
    #[clap(long, default_value = "1")]
    pub num_senders: usize,

    #[clap(long, default_value = "1")]
    pub num_receivers_per_sender: usize,

    #[clap(long, default_value = "10000")]
    pub num_messages: usize,

    #[clap(long, default_value = "1000")]
    pub hwm: i32,

    #[clap(long, default_value = "500")]
    pub batch_sleep_ms: u64,

    #[clap(long, default_value = "1")]
    pub num_dealer_pairs: usize,

    #[clap(long, default_value = "1")]
    pub num_dealerrouter_dealers: usize,

    #[clap(long, value_delimiter = ',', default_value = "8,64,256,1024,4096")]
    pub payload_sizes: Vec<usize>,
}

fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();

    match args.command {
        Command::PubsubBenchmark(args) => {
            let total_tasks = args.num_senders * (1 + args.num_receivers_per_sender);
            let worker_threads = total_tasks.min(num_cpus::get());
            let rt = Builder::new_multi_thread()
                .worker_threads(worker_threads)
                .enable_all()
                .build()?;
            rt.block_on(run_pubsub_benchmark(args))?;
        }
        Command::DealerBenchmark(args) => {
            let total_tasks = args.num_pairs * 2;
            let worker_threads = total_tasks.min(num_cpus::get());
            let rt = Builder::new_multi_thread()
                .worker_threads(worker_threads)
                .enable_all()
                .build()?;
            rt.block_on(run_dealer_benchmark(args))?;
        }
        Command::DealerRouterBenchmark(args) => {
            let total_tasks = args.num_dealers * 2 + 1;
            let worker_threads = total_tasks.min(num_cpus::get());
            let rt = Builder::new_multi_thread()
                .worker_threads(worker_threads)
                .enable_all()
                .build()?;
            rt.block_on(run_dealerrouter_benchmark(args))?;
        }
        Command::RunBenchmarks(args) => {
            let rt = Builder::new_multi_thread()
                .worker_threads(num_cpus::get())
                .enable_all()
                .build()?;
            rt.block_on(run_benchmarks(args))?;
        }
    }

    Ok(())
}

async fn run_pubsub_benchmark(args: PubsubBenchmarkArgs) -> Result<(), Box<dyn Error>> {
    println!(
        "Starting benchmark with {} senders and {} receivers per sender",
        args.num_senders, args.num_receivers_per_sender
    );

    println!("Calibrating TSC...");
    let tsc_per_ns = calibrate_tsc();
    println!(
        "TSC calibration: {:.3} GHz ({:.6} cycles/ns)",
        tsc_per_ns, tsc_per_ns
    );

    let mut subscriber_tasks = Vec::new();
    let mut publisher_tasks = Vec::new();

    for sender_id in 0..args.num_senders {
        let address = if args.transport == "ipc" {
            format!("ipc:///tmp/pub_{}.ipc", sender_id)
        } else {
            format!("tcp://127.0.0.1:{}", 5000 + sender_id)
        };

        for _receiver_id in 0..args.num_receivers_per_sender {
            let sub_args = subscriber::Args {
                addresses: vec![address.clone()],
                num_messages: args.num_messages,
                payload_size: args.payload_size,
                hwm: args.hwm,
            };

            let task =
                tokio::spawn(async move { subscriber::run_async(sub_args, tsc_per_ns).await });
            subscriber_tasks.push(task);
        }
    }

    for sender_id in 0..args.num_senders {
        let address = if args.transport == "ipc" {
            format!("ipc:///tmp/pub_{}.ipc", sender_id)
        } else {
            format!("tcp://127.0.0.1:{}", 5000 + sender_id)
        };

        let pub_args = publisher::Args {
            payload_size: args.payload_size,
            num_messages: args.num_messages,
            address,
            hwm: args.hwm,
            batch_sleep_ms: args.batch_sleep_ms,
        };

        let task = tokio::spawn(async move { publisher::run_async(pub_args).await });
        publisher_tasks.push(task);
    }

    let mut histograms: Vec<Histogram<u64>> = Vec::new();
    for task in subscriber_tasks {
        match task.await {
            Ok(Ok(h)) => histograms.push(h),
            Ok(Err(e)) => return Err(format!("Subscriber task failed: {}", e).into()),
            Err(e) => return Err(format!("Subscriber task join error: {}", e).into()),
        }
    }

    for task in publisher_tasks {
        match task.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => return Err(format!("Publisher task failed: {}", e).into()),
            Err(e) => return Err(format!("Publisher task join error: {}", e).into()),
        }
    }

    let mut total_histogram = Histogram::<u64>::new(3)?;
    for h in histograms {
        total_histogram.add(h)?;
    }
    aggregator::append_benchmark_result(
        "PubSub",
        &args.transport.to_uppercase(),
        args.payload_size,
        &total_histogram,
    )?;

    println!("Benchmark complete!");
    Ok(())
}

async fn run_dealer_benchmark(args: DealerBenchmarkArgs) -> Result<(), Box<dyn Error>> {
    println!("Starting dealer benchmark with {} pairs", args.num_pairs);

    println!("Calibrating TSC...");
    let tsc_per_ns = calibrate_tsc();
    println!(
        "TSC calibration: {:.3} GHz ({:.6} cycles/ns)",
        tsc_per_ns, tsc_per_ns
    );

    let mut receiver_tasks = Vec::new();
    let mut sender_tasks = Vec::new();

    for pair_id in 0..args.num_pairs {
        let bind_address = if args.transport == "ipc" {
            format!("ipc:///tmp/dealer_{}.ipc", pair_id)
        } else {
            format!("tcp://127.0.0.1:{}", 6000 + pair_id)
        };

        let recv_args = dealer_receiver::Args {
            payload_size: args.payload_size,
            num_messages: args.num_messages,
            bind_address,
        };

        let task =
            tokio::spawn(async move { dealer_receiver::run_async(recv_args, tsc_per_ns).await });
        receiver_tasks.push(task);
    }

    for pair_id in 0..args.num_pairs {
        let receiver_address = if args.transport == "ipc" {
            format!("ipc:///tmp/dealer_{}.ipc", pair_id)
        } else {
            format!("tcp://127.0.0.1:{}", 6000 + pair_id)
        };

        let send_args = dealer_sender::Args {
            payload_size: args.payload_size,
            num_messages: args.num_messages,
            receiver_address,
        };

        let task = tokio::spawn(async move { dealer_sender::run_async(send_args).await });
        sender_tasks.push(task);
    }

    let mut histograms: Vec<Histogram<u64>> = Vec::new();
    for task in receiver_tasks {
        match task.await {
            Ok(Ok(h)) => histograms.push(h),
            Ok(Err(e)) => return Err(format!("Receiver task failed: {}", e).into()),
            Err(e) => return Err(format!("Receiver task join error: {}", e).into()),
        }
    }

    for task in sender_tasks {
        match task.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => return Err(format!("Sender task failed: {}", e).into()),
            Err(e) => return Err(format!("Sender task join error: {}", e).into()),
        }
    }

    let mut total_histogram = Histogram::<u64>::new(3)?;
    for h in histograms {
        total_histogram.add(h)?;
    }
    aggregator::append_benchmark_result(
        "Dealer",
        &args.transport.to_uppercase(),
        args.payload_size,
        &total_histogram,
    )?;

    println!("Dealer benchmark complete!");
    Ok(())
}

async fn run_dealerrouter_benchmark(args: DealerRouterBenchmarkArgs) -> Result<(), Box<dyn Error>> {
    println!(
        "Starting dealer-router benchmark with {} dealers",
        args.num_dealers
    );

    let tsc_per_ns = calibrate_tsc();
    println!(
        "TSC calibration: {:.3} GHz ({:.6} cycles/ns)",
        tsc_per_ns, tsc_per_ns
    );

    let end_barrier = Arc::new(Barrier::new(args.num_dealers + 1));

    let router_address = if args.transport == "ipc" {
        "ipc:///tmp/dealerrouter.ipc".to_string()
    } else {
        "tcp://127.0.0.1:7000".to_string()
    };

    let router_args = dealerrouter_router::Args {
        bind_address: router_address.clone(),
        num_dealers: args.num_dealers,
        num_messages_per_dealer: args.num_messages,
        hwm: args.hwm,
    };

    let end_barrier_clone = end_barrier.clone();
    let router_task = tokio::spawn(async move {
        dealerrouter_router::run_async(router_args, end_barrier_clone).await
    });

    let mut dealer_tasks = Vec::new();
    for dealer_id in 0..args.num_dealers {
        let dealer_args = dealerrouter_dealer::Args {
            payload_size: args.payload_size,
            num_messages: args.num_messages,
            hwm: args.hwm,
            batch_sleep_ms: args.batch_sleep_ms,
            router_address: router_address.clone(),
            dealer_id,
            num_dealers: args.num_dealers,
        };

        let end_barrier_clone = end_barrier.clone();
        let task = tokio::spawn(async move {
            dealerrouter_dealer::run_async(dealer_args, end_barrier_clone, tsc_per_ns).await
        });
        dealer_tasks.push(task);
    }

    let mut histograms: Vec<Histogram<u64>> = Vec::new();
    for task in dealer_tasks {
        match task.await {
            Ok(Ok(h)) => histograms.push(h),
            Ok(Err(e)) => return Err(format!("Dealer task failed: {}", e).into()),
            Err(e) => return Err(format!("Dealer task join error: {}", e).into()),
        }
    }

    match router_task.await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => return Err(format!("Router task failed: {}", e).into()),
        Err(e) => return Err(format!("Router task join error: {}", e).into()),
    }

    let mut total_histogram = Histogram::<u64>::new(3)?;
    for h in histograms {
        total_histogram.add(h)?;
    }
    aggregator::append_benchmark_result(
        "DealerRouter",
        &args.transport.to_uppercase(),
        args.payload_size,
        &total_histogram,
    )?;

    println!("Dealer-router benchmark complete!");
    Ok(())
}

async fn run_benchmarks(args: RunBenchmarksArgs) -> Result<(), Box<dyn Error>> {
    // Fresh start for full suite (matches original script behavior)
    let _ = std::fs::remove_file("results.csv");
    if let Ok(entries) = std::fs::read_dir(".") {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().map_or(false, |e| e == "hgrm") {
                let _ = std::fs::remove_file(&path);
            }
        }
    }

    println!("Starting benchmarks...");
    println!(
        "PUB/SUB Configuration: {} senders, {} receivers per sender, {} messages",
        args.num_senders, args.num_receivers_per_sender, args.num_messages
    );
    println!(
        "DEALER Configuration: {} pairs, {} messages",
        args.num_dealer_pairs, args.num_messages
    );
    println!(
        "DEALER-ROUTER Configuration: {} dealers (circular), {} messages (per dealer), High Water Mark: {}",
        args.num_dealerrouter_dealers, args.num_messages, args.hwm
    );
    println!("Batch sleep: {}ms (PUB/SUB only)", args.batch_sleep_ms);
    println!("");

    println!("=== PUB/SUB Benchmarks ===");
    for size in &args.payload_sizes {
        println!("Running PUB/SUB IPC - Payload: {} bytes", size);

        let bargs = PubsubBenchmarkArgs {
            num_senders: args.num_senders,
            num_receivers_per_sender: args.num_receivers_per_sender,
            num_messages: args.num_messages,
            payload_size: *size,
            transport: "ipc".to_string(),
            hwm: args.hwm,
            batch_sleep_ms: args.batch_sleep_ms,
        };
        run_pubsub_benchmark(bargs).await?;

        for i in 0..args.num_senders {
            let _ = std::fs::remove_file(format!("/tmp/pub_{}.ipc", i));
        }

        println!("  Completed PUB/SUB IPC - Payload: {} bytes", size);
        println!("");
    }

    for size in &args.payload_sizes {
        println!("Running PUB/SUB TCP - Payload: {} bytes", size);

        let bargs = PubsubBenchmarkArgs {
            num_senders: args.num_senders,
            num_receivers_per_sender: args.num_receivers_per_sender,
            num_messages: args.num_messages,
            payload_size: *size,
            transport: "tcp".to_string(),
            hwm: args.hwm,
            batch_sleep_ms: args.batch_sleep_ms,
        };
        run_pubsub_benchmark(bargs).await?;

        println!("  Completed PUB/SUB TCP - Payload: {} bytes", size);
        println!("");
    }

    println!("=== DEALER Benchmarks ===");
    for size in &args.payload_sizes {
        println!("Running DEALER IPC - Payload: {} bytes", size);

        let bargs = DealerBenchmarkArgs {
            num_pairs: args.num_dealer_pairs,
            num_messages: args.num_messages,
            payload_size: *size,
            transport: "ipc".to_string(),
        };
        run_dealer_benchmark(bargs).await?;

        for i in 0..args.num_dealer_pairs {
            let _ = std::fs::remove_file(format!("/tmp/dealer_{}.ipc", i));
        }

        println!("  Completed DEALER IPC - Payload: {} bytes", size);
        println!("");
    }

    for size in &args.payload_sizes {
        println!("Running DEALER TCP - Payload: {} bytes", size);

        let bargs = DealerBenchmarkArgs {
            num_pairs: args.num_dealer_pairs,
            num_messages: args.num_messages,
            payload_size: *size,
            transport: "tcp".to_string(),
        };
        run_dealer_benchmark(bargs).await?;

        println!("  Completed DEALER TCP - Payload: {} bytes", size);
        println!("");
    }

    println!("=== DEALER-ROUTER Benchmarks ===");
    for size in &args.payload_sizes {
        println!("Running DEALER-ROUTER IPC - Payload: {} bytes", size);

        let bargs = DealerRouterBenchmarkArgs {
            num_dealers: args.num_dealerrouter_dealers,
            num_messages: args.num_messages,
            payload_size: *size,
            transport: "ipc".to_string(),
            hwm: args.hwm,
            batch_sleep_ms: args.batch_sleep_ms,
        };
        run_dealerrouter_benchmark(bargs).await?;

        let _ = std::fs::remove_file("/tmp/dealerrouter.ipc");

        println!("  Completed DEALER-ROUTER IPC - Payload: {} bytes", size);
        println!("");
    }

    for size in &args.payload_sizes {
        println!("Running DEALER-ROUTER TCP - Payload: {} bytes", size);

        let bargs = DealerRouterBenchmarkArgs {
            num_dealers: args.num_dealerrouter_dealers,
            num_messages: args.num_messages,
            payload_size: *size,
            transport: "tcp".to_string(),
            hwm: args.hwm,
            batch_sleep_ms: args.batch_sleep_ms,
        };
        run_dealerrouter_benchmark(bargs).await?;

        println!("  Completed DEALER-ROUTER TCP - Payload: {} bytes", size);
        println!("");
    }

    println!("All benchmarks complete! Results in results.csv");
    Ok(())
}
