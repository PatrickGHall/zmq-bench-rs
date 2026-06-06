use clap::Parser;
use hdrhistogram::Histogram;
use std::error::Error;
use tokio::runtime::Builder;
mod roles;
mod zmq_helpers;

use crate::roles::{Pattern, cleanup_ipc, launch_dealer, launch_dealerrouter, launch_pubsub};
use crate::zmq_helpers::{
    collect_and_append_result, get_tsc_per_ns, print_per_item_stats, SyncPhase,
    cleanup_dirty_state, assign_next_cpu, pin_current_thread_to_cpu,
    context, init_context,
};

// fatal + dirty cleanup on early exit: ensures no stale ipc left that would hang future runs.
fn fatal(context: &str, err: impl std::fmt::Display) -> ! {
    eprintln!("FATAL: {}: {}", context, err);
    cleanup_dirty_state();
    std::process::exit(1);
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

    #[clap(long)]
    pub save_hists: bool,

    #[clap(long, default_value = "results.csv")]
    pub output: String,
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

    #[clap(long)]
    pub save_hists: bool,

    #[clap(long, default_value = "results.csv")]
    pub output: String,
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

    #[clap(long)]
    pub save_hists: bool,

    #[clap(long, default_value = "results.csv")]
    pub output: String,
}

#[derive(Parser, Debug)]
struct RunBenchmarksArgs {
    #[clap(long, default_value = "1")]
    pub num_senders: usize,

    #[clap(long, default_value = "1")]
    pub num_receivers_per_sender: usize,

    #[clap(long, default_value = "10000")]
    pub num_messages: usize,

    #[clap(long, default_value = "1")]
    pub num_dealer_pairs: usize,

    #[clap(long, default_value = "1")]
    pub num_dealerrouter_dealers: usize,

    #[clap(long, value_delimiter = ',', default_value = "8,64,256,1024,4096")]
    pub payload_sizes: Vec<usize>,

    #[clap(long)]
    pub save_hists: bool,

    #[clap(long, default_value = "results.csv")]
    pub output: String,
}



fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();

    match args.command {
        Command::PubsubBenchmark(a) => {
            init_context(a.save_hists, a.output);
            let p = Pattern::PubSub {
                num_senders: a.num_senders,
                num_receivers_per_sender: a.num_receivers_per_sender,
            };
            let total = a.num_senders * (1 + a.num_receivers_per_sender);
            run_bench_with_pattern(p, a.transport, a.payload_size, a.num_messages, total)?;
        }
        Command::DealerBenchmark(a) => {
            init_context(a.save_hists, a.output);
            let p = Pattern::Dealer { num_pairs: a.num_pairs };
            let total = a.num_pairs * 2;
            run_bench_with_pattern(p, a.transport, a.payload_size, a.num_messages, total)?;
        }
        Command::DealerRouterBenchmark(a) => {
            init_context(a.save_hists, a.output);
            let p = Pattern::DealerRouter { num_dealers: a.num_dealers };
            let total = a.num_dealers * 2 + 1;
            run_bench_with_pattern(p, a.transport, a.payload_size, a.num_messages, total)?;
        }
        Command::RunBenchmarks(args) => {
            init_context(args.save_hists, args.output.clone());
            let rt = build_bench_runtime(num_cpus::get())?;
            rt.block_on(run_benchmarks(args))?;
        }
    }

    Ok(())
}

fn build_bench_runtime(worker_threads: usize) -> Result<tokio::runtime::Runtime, Box<dyn Error>> {
    let mut builder = Builder::new_multi_thread();
    builder
        .worker_threads(worker_threads)
        .enable_all();

    builder.on_thread_start(|| {
        let cpu = assign_next_cpu();
        let _ = pin_current_thread_to_cpu(cpu);
    });

    builder.build().map_err(Into::into)
}

fn run_bench_with_pattern(
    p: Pattern,
    transport: String,
    payload_size: usize,
    num_messages: usize,
    total_tasks_hint: usize,
) -> Result<(), Box<dyn Error>> {
    let threads = total_tasks_hint.min(num_cpus::get());
    let rt = build_bench_runtime(threads)?;
    rt.block_on(run_benchmark(p, transport, payload_size, num_messages))
}

async fn run_benchmark(
    pattern: Pattern,
    transport: String,
    payload_size: usize,
    num_messages: usize,
) -> Result<(), Box<dyn Error>> {
    println!("{}", pattern.start_message());

    let tsc_per_ns = get_tsc_per_ns();

    let sync = SyncPhase::new();

    let (hist_tasks, completion_tasks) = match &pattern {
        Pattern::PubSub { num_senders, num_receivers_per_sender } => {
            launch_pubsub(
                *num_senders, *num_receivers_per_sender,
                &transport, payload_size, num_messages, &sync,
            )
        }
        Pattern::Dealer { num_pairs } => {
            launch_dealer(
                *num_pairs, &transport, payload_size, num_messages, &sync,
            )
        }
        Pattern::DealerRouter { num_dealers } => {
            launch_dealerrouter(
                *num_dealers, &transport, payload_size, num_messages, &sync,
            )
        }
    };

    let mut histograms: Vec<Histogram<u64>> = Vec::new();
    for task in hist_tasks {
        match task.await {
            Ok(Ok(h)) => histograms.push(h),
            Ok(Err(e)) => fatal("Receiver/dealer task failed", e),
            Err(e) => fatal("Task join error (hist)", e),
        }
    }

    if !pattern.per_item_label().is_empty() {
        print_per_item_stats(pattern.per_item_label(), &histograms);
    }

    for task in completion_tasks {
        match task.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => fatal("Sender/router task failed", e),
            Err(e) => fatal("Task join error (completion)", e),
        }
    }

    sync.print(tsc_per_ns);
    collect_and_append_result(pattern.name(), &transport.to_uppercase(), payload_size, histograms)?;

    println!("{} benchmark complete!", pattern.name());
    Ok(())
}

async fn run_benchmarks(args: RunBenchmarksArgs) -> Result<(), Box<dyn Error>> {
    cleanup_dirty_state();
    let _ = std::fs::remove_file(&context().output);
    if let Ok(entries) = std::fs::read_dir(".") {
        for entry in entries.flatten() {
            let path = entry.path();
            let ext = path.extension().and_then(|e| e.to_str()).unwrap_or("");
            let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
            if ext == "hgrm" || ext == "rawlat.txt" || name.starts_with("indiv_") {
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
        "DEALER-ROUTER Configuration: {} dealers (circular), {} messages (per dealer)",
        args.num_dealerrouter_dealers, args.num_messages
    );
    println!("HWM internally = num_messages + headroom (no drops possible). No batch-sleep (lean, full-speed after handshake).");
    println!();

    let sections: &[(&str, Pattern)] = &[
        ("PUB/SUB", Pattern::PubSub {
            num_senders: args.num_senders,
            num_receivers_per_sender: args.num_receivers_per_sender,
        }),
        ("DEALER", Pattern::Dealer { num_pairs: args.num_dealer_pairs }),
        ("DEALER-ROUTER", Pattern::DealerRouter { num_dealers: args.num_dealerrouter_dealers }),
    ];

    for (label, pattern) in sections {
        println!("=== {} Benchmarks ===", label);
        for transport in ["ipc", "tcp"] {
            for &size in &args.payload_sizes {
                let do_cleanup = || {
                    if transport == "ipc" {
                        let (prefix, count) = match *label {
                            "PUB/SUB" => ("pub", args.num_senders),
                            "DEALER" => ("dealer", args.num_dealer_pairs),
                            "DEALER-ROUTER" => ("dealerrouter", 0),
                            _ => ("", 0),
                        };
                        cleanup_ipc(prefix, count);
                    }
                };
                do_cleanup();

                println!("Running {} {} - Payload: {} bytes", label, transport.to_uppercase(), size);
                run_benchmark(pattern.clone(), transport.to_string(), size, args.num_messages).await?;

                do_cleanup();

                println!("  Completed {} {} - Payload: {} bytes", label, transport.to_uppercase(), size);
                println!();
            }
        }
    }

    println!("All benchmarks complete! Results in {}", context().output);
    Ok(())
}
