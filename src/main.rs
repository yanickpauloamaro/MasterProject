extern crate hwloc;
extern crate anyhow;
extern crate tokio;
extern crate either;

use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::sync::Arc;
use crossbeam_utils::thread;
use testbench::config::{BenchmarkConfig, Config, ConfigFile};
use testbench::benchmark::{BasicWorkload, Benchmark, benchmarking, ConflictWorkload, ContentionWorkload, TransactionLoop};
use anyhow::{Context, Result};
use tokio::runtime::Runtime;
use testbench::vm_implementation::{VMa, VmMemory, VmType};
use testbench::wip::numa_latency;
use core_affinity;
use testbench::transaction::Transaction;
use testbench::utils::transfer;
use rand::seq::SliceRandom;

fn main() -> Result<()>{
    println!("Hello, world!");

    // let config = Config::new("config_single_batch.json")
    //     .context("Unable to create benchmark config")?;

    // let core_ids = core_affinity::get_core_ids().unwrap();
    // println!("Core ids: {:?}", core_ids);

    // let rt = Runtime::new().unwrap();
    // let _guard = rt.enter();

    // let _ = BasicWorkload::run(config, 1).await;
    // let _ = ContentionWorkload::run(config, 1).await;
    // let _ = TransactionLoop::run(config, 1).await;
    // let _ = ConflictWorkload::run(config, 1);
    benchmarking("benchmark_config.json")?;

    println!("See you, world!");

    Ok(())
}

fn profiling() -> Result<()> {
    let config = BenchmarkConfig::new("benchmark_config.json")
        .context("Unable to create benchmark config")?;

    let start = Instant::now();
    let batch_size = config.batch_sizes[0];
    let memory_size = batch_size * 2;
    let nb_cores = config.nb_cores[0];
    let conflict_rate = config.conflict_rates[0];

    {
        // let mut vm = VmFactory::new_vm(
        //     &config.vm_types[0],
        //     memory_size,
        //     nb_cores,
        //     batch_size
        // );
        //
        // vm.set_memory(3 * nb_repetitions as u64);
        // let batch = batch_with_conflicts(batch_size, conflict_rate);
        //
        // // for _ in 0..config.repetitions {
        // vm.execute(batch);
        // // }
        // println!("execution latency: {:?}", elapsed.div(config.repetitions as u32));
    }

    {
        let batch = batch_with_conflicts(batch_size, conflict_rate);
        let mut backlog = Vec::with_capacity(batch.len());
        let mut address_to_worker = vec![usize::MAX; memory_size];

        for _ in 0..config.repetitions {
            address_to_worker.fill(NONE);
            let assignment = assign_workers(
                nb_cores,
                &batch,
                &mut address_to_worker,
                &mut backlog
            );
        }

        let elapsed = start.elapsed();
        println!("assign_workers average latency: {:?}", elapsed.div(config.repetitions as u32));
        println!("Total {} runs: {:?}", config.repetitions, elapsed);
    }

    Ok(())
}