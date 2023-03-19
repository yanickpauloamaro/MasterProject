extern crate hwloc;
extern crate anyhow;
extern crate tokio;
extern crate either;

use std::collections::{HashMap, HashSet};
use std::collections::hash_map::DefaultHasher;
use std::hash::Hash;
use std::ops::{Add, Div};
use std::sync::Arc;
use std::time::Duration;
use crossbeam_utils::thread;
use testbench::config::{BenchmarkConfig, Config, ConfigFile};
use testbench::benchmark::{BasicWorkload, Benchmark, benchmarking, ConflictWorkload, ContentionWorkload, TransactionLoop};
use anyhow::{Context, Result};
use tokio::runtime::Runtime;
use testbench::vm_implementation::{VMa, VmFactory, VmMemory, VmType, WorkerIndex};
use testbench::wip::{assign_workers, assign_workers_dummy_modulo, assign_workers_dummy_modulo_u16, assign_workers_new_impl, assign_workers_new_impl_2, assign_workers_original, AssignedWorker, NONE_TEST, NONE_WIP, numa_latency};
use core_affinity;
use testbench::transaction::Transaction;
use testbench::utils::{batch_with_conflicts, transfer};
use rand::seq::SliceRandom;
use tokio::time::Instant;
use ed25519_dalek::{Sha512, Digest};

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
    return Ok(());

    // profiling(1);
    let config = BenchmarkConfig::new("benchmark_config.json")
        .context("Unable to create benchmark config")?;

    let start = Instant::now();
    let batch_size = config.batch_sizes[0];
    let memory_size = batch_size * 2;
    let nb_cores = config.nb_cores[0];
    let conflict_rate = config.conflict_rates[0];

    let batch = batch_with_conflicts(batch_size, conflict_rate);
    let mut backlog = Vec::with_capacity(batch.len());

    let reduced_vm_size = memory_size;
    // let reduced_vm_size = memory_size >> 1; // 50%       = 65536
    // let reduced_vm_size = memory_size >> 2; // 25%       = 32768
    // let reduced_vm_size = memory_size >> 3; // 12.5%     = 16384
    // let reduced_vm_size = memory_size >> 4; // 6.25%     = 8192
    // let reduced_vm_size = memory_size >> 5; // 3...%     = 4096
    // let reduced_vm_size = memory_size >> 6; // 1.5...%   = 2048
    // let reduced_vm_size = memory_size >> 7; // 0.7...%   = 1024

    let mut s = DefaultHasher::new();
    let mut address_to_worker = vec![0; reduced_vm_size];

    let mut latency_sum = Duration::from_nanos(0);
    for _ in 0..config.repetitions {
        address_to_worker.fill(0);
        let a = Instant::now();
        let assignment = assign_workers_original(
            nb_cores,
            &batch,
            &mut address_to_worker,
            &mut backlog,
            // &mut s
        );
        latency_sum = latency_sum.add(a.elapsed());
        // println!("Assignment: {:?}", address_to_worker);
    }

    let elapsed = start.elapsed();
    println!("Total {} runs: {:?}", config.repetitions, elapsed);
    println!("Average latency: {:?}", elapsed.div(config.repetitions as u32));

    println!("Total {} runs: {:?} (acc)", config.repetitions, latency_sum);
    println!("Average latency (acc): {:?}", latency_sum.div(config.repetitions as u32));

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
        let mut address_to_worker = vec![NONE_TEST; memory_size];

        for _ in 0..config.repetitions {
            address_to_worker.fill(NONE_TEST);
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