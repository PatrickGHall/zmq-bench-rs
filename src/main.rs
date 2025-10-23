use clap::Parser;
use std::arch::x86_64::_rdtsc;
use std::error::Error;
use std::time::{Duration, SystemTime};
use tokio::runtime::Builder;

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
    Aggregator(aggregator::Args),
    PubsubBenchmark(PubsubBenchmarkArgs),
    DealerBenchmark(DealerBenchmarkArgs),
    DealerRouterBenchmark(DealerRouterBenchmarkArgs),
}

#[derive(Parser, Debug)]
struct PubsubBenchmarkArgs {
    #[clap(long, default_value = "1")]
    pub num_senders: usize,

    #[clap(long, default_value = "1")]
    pub num_receivers_per_sender: usize,

    #[clap(long, default_value = "100000")]
    pub num_messages: usize,

    #[clap(long, default_value = "64")]
    pub payload_size: usize,

    #[clap(long, default_value = "ipc")]
    pub transport: String,

    #[clap(long)]
    pub hwm: Option<i32>,

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
    #[clap(long, default_value = "2")]
    pub num_dealers: usize,

    #[clap(long, default_value = "10000")]
    pub num_messages: usize,

    #[clap(long, default_value = "64")]
    pub payload_size: usize,

    #[clap(long, default_value = "ipc")]
    pub transport: String,

    #[clap(long)]
    pub hwm: Option<i32>,
}

fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();

    match args.command {
        Command::Aggregator(args) => aggregator::run(args)?,
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
    }

    Ok(())
}

async fn run_pubsub_benchmark(args: PubsubBenchmarkArgs) -> Result<(), Box<dyn Error>> {
    use std::sync::Arc;
    use tokio::sync::Barrier;

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

    let total_publishers = args.num_senders;
    let total_subscribers = args.num_senders * args.num_receivers_per_sender;
    let total_participants = total_publishers + total_subscribers;

    let barrier = Arc::new(Barrier::new(total_participants));

    let mut subscriber_tasks = Vec::new();
    let mut publisher_tasks = Vec::new();

    for sender_id in 0..args.num_senders {
        let address = if args.transport == "ipc" {
            format!("ipc:///tmp/pub_{}.ipc", sender_id)
        } else {
            format!("tcp://127.0.0.1:{}", 5000 + sender_id)
        };

        for receiver_id in 0..args.num_receivers_per_sender {
            let sub_args = subscriber::Args {
                addresses: vec![address.clone()],
                num_messages: args.num_messages,
                id: format!("sub_{}_{}", sender_id, receiver_id),
                benchmark_name: format!("PubSub-{}", args.transport.to_uppercase()),
                payload_size: args.payload_size,
                hwm: args.hwm,
            };

            let barrier_clone = barrier.clone();
            let task = tokio::spawn(async move {
                subscriber::run_async(sub_args, tsc_per_ns, barrier_clone).await
            });
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

        let barrier_clone = barrier.clone();
        let task = tokio::spawn(async move { publisher::run_async(pub_args, barrier_clone).await });
        publisher_tasks.push(task);
    }

    for task in subscriber_tasks {
        match task.await {
            Ok(Ok(())) => {}
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

    println!("Benchmark complete!");
    Ok(())
}

async fn run_dealer_benchmark(args: DealerBenchmarkArgs) -> Result<(), Box<dyn Error>> {
    use std::sync::Arc;
    use tokio::sync::Barrier;

    println!("Starting dealer benchmark with {} pairs", args.num_pairs);

    println!("Calibrating TSC...");
    let tsc_per_ns = calibrate_tsc();
    println!(
        "TSC calibration: {:.3} GHz ({:.6} cycles/ns)",
        tsc_per_ns, tsc_per_ns
    );

    let barrier = Arc::new(Barrier::new(args.num_pairs + 1));

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
            id: pair_id,
            benchmark_name: format!("Dealer-{}", args.transport.to_uppercase()),
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
            id: pair_id,
        };

        let barrier_clone = barrier.clone();
        let task =
            tokio::spawn(async move { dealer_sender::run_async(send_args, barrier_clone).await });
        sender_tasks.push(task);
    }

    barrier.wait().await;

    for task in receiver_tasks {
        match task.await {
            Ok(Ok(())) => {}
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

    println!("Dealer benchmark complete!");
    Ok(())
}

async fn run_dealerrouter_benchmark(args: DealerRouterBenchmarkArgs) -> Result<(), Box<dyn Error>> {
    use std::sync::Arc;
    use tokio::sync::Barrier;

    println!(
        "Starting dealer-router benchmark with {} dealers",
        args.num_dealers
    );

    let tsc_per_ns = calibrate_tsc();
    println!(
        "TSC calibration: {:.3} GHz ({:.6} cycles/ns)",
        tsc_per_ns, tsc_per_ns
    );

    let start_barrier = Arc::new(Barrier::new(args.num_dealers + 1));
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

    let start_barrier_clone = start_barrier.clone();
    let end_barrier_clone = end_barrier.clone();
    let router_task = tokio::spawn(async move {
        dealerrouter_router::run_async(router_args, start_barrier_clone, end_barrier_clone).await
    });

    let mut dealer_tasks = Vec::new();
    for dealer_id in 0..args.num_dealers {
        let dealer_args = dealerrouter_dealer::Args {
            payload_size: args.payload_size,
            num_messages: args.num_messages,
            router_address: router_address.clone(),
            dealer_id,
            num_dealers: args.num_dealers,
            benchmark_name: format!("DealerRouter-{}", args.transport.to_uppercase()),
            hwm: args.hwm,
        };

        let start_barrier_clone = start_barrier.clone();
        let end_barrier_clone = end_barrier.clone();
        let task = tokio::spawn(async move {
            dealerrouter_dealer::run_async(
                dealer_args,
                start_barrier_clone,
                end_barrier_clone,
                tsc_per_ns,
            )
            .await
        });
        dealer_tasks.push(task);
    }

    for task in dealer_tasks {
        match task.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => return Err(format!("Dealer task failed: {}", e).into()),
            Err(e) => return Err(format!("Dealer task join error: {}", e).into()),
        }
    }

    match router_task.await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => return Err(format!("Router task failed: {}", e).into()),
        Err(e) => return Err(format!("Router task join error: {}", e).into()),
    }

    println!("Dealer-router benchmark complete!");
    Ok(())
}
