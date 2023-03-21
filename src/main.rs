// #![allow(unused_imports)]
extern crate anyhow;
extern crate either;
extern crate hwloc;
extern crate tokio;

use std::ops::{Add, Div};
use std::time::Duration;

use anyhow::{Context, Result};
use rand::rngs::StdRng;
use rand::SeedableRng;
// use core_affinity;
use tokio::time::Instant;

use testbench::benchmark::benchmarking;
use testbench::config::{BenchmarkConfig, ConfigFile};
use testbench::transaction::Transaction;
use testbench::utils::batch_with_conflicts;
use testbench::vm_utils::{assign_workers, UNASSIGNED};

fn main() -> Result<()>{
    println!("Hello, world!");

    // let _ = BasicWorkload::run(config, 1).await;
    // let _ = ContentionWorkload::run(config, 1).await;
    // let _ = TransactionLoop::run(config, 1).await;
    // let _ = ConflictWorkload::run(config, 1);

    benchmarking("benchmark_config.json")?;
    // profiling("benchmark_config.json")?;

    return Ok(());
}

#[allow(dead_code)]
fn profiling(path: &str) -> Result<()> {

    let config = BenchmarkConfig::new(path)
        .context("Unable to create benchmark config")?;

    let start = Instant::now();
    let batch_size = config.batch_sizes[0];
    let memory_size = batch_size * 2;
    let nb_cores = config.nb_cores[0];
    let conflict_rate = config.conflict_rates[0];

    let mut rng = match config.seed {
        Some(seed) => {
            StdRng::seed_from_u64(seed)
        },
        None => StdRng::seed_from_u64(rand::random())
    };

    let batch: Vec<Transaction> = batch_with_conflicts(batch_size, conflict_rate, &mut rng);
    let mut backlog: Vec<Transaction> = Vec::with_capacity(batch.len());

    let reduced_vm_size = memory_size;
    // let reduced_vm_size = memory_size >> 1; // 50%       = 65536
    // let reduced_vm_size = memory_size >> 2; // 25%       = 32768
    // let reduced_vm_size = memory_size >> 3; // 12.5%     = 16384
    // let reduced_vm_size = memory_size >> 4; // 6.25%     = 8192
    // let reduced_vm_size = memory_size >> 5; // 3...%     = 4096
    // let reduced_vm_size = memory_size >> 6; // 1.5...%   = 2048
    // let reduced_vm_size = memory_size >> 7; // 0.7...%   = 1024

    // let mut s = DefaultHasher::new();
    let mut address_to_worker = vec![UNASSIGNED; reduced_vm_size];
    // let mut address_to_worker = HashMap::new();

    let mut latency_sum = Duration::from_nanos(0);
    for _i in 0..config.repetitions {
        address_to_worker.fill(UNASSIGNED);

        let a = Instant::now();
        let _assignment = assign_workers(
            nb_cores,
            &batch,
            &mut address_to_worker,
            &mut backlog,
            // &mut s
        );

        latency_sum = latency_sum.add(a.elapsed());
    }

    let elapsed = start.elapsed();
    println!("Total {} runs: {:?}", config.repetitions, elapsed);
    println!("Average latency: {:?}", elapsed.div(config.repetitions as u32));

    println!("Total {} runs: {:?} (acc)", config.repetitions, latency_sum);
    println!("Average latency (acc): {:?}", latency_sum.div(config.repetitions as u32));

    println!("See you, world!");

    Ok(())
}