#![allow(unused_imports)]
extern crate anyhow;
extern crate either;
extern crate hwloc;
extern crate tokio;

use std::ops::{Add, Div};
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context, Result};
use rand::rngs::StdRng;
use rand::SeedableRng;
// use core_affinity;

use testbench::benchmark::benchmarking;
use testbench::config::{BenchmarkConfig, ConfigFile};
use testbench::transaction::Transaction;
use testbench::utils::{batch_with_conflicts, batch_with_conflicts_new_impl};
use testbench::vm::{ExecutionResult, Executor};
use testbench::vm_c::VMc;
use testbench::vm_utils::{assign_workers, UNASSIGNED, VmStorage};
use testbench::wip::{assign_workers_new_impl, assign_workers_new_impl_2};
use testbench::worker_implementation::WorkerC;

fn main() -> Result<()>{
    println!("Hello, world!");

    // let _ = BasicWorkload::run(config, 1).await;
    // let _ = ContentionWorkload::run(config, 1).await;
    // let _ = TransactionLoop::run(config, 1).await;
    // let _ = ConflictWorkload::run(config, 1);

    // benchmarking("benchmark_config.json")?;
    profiling("benchmark_config.json")?;

    // let _ = crossbeam::scope(|s| {
    //     let start = Instant::now();
    //     let mut handles = vec!();
    //     for i in 0..7 {
    //         handles.push(s.spawn(move |_| {
    //             println!("Spawn worker {}", i);
    //             while start.elapsed().as_secs() < 10 {
    //
    //             }
    //             println!("Worker {} done after {:?}", i, start.elapsed());
    //         }));
    //     }
    // }).or(Err(anyhow::anyhow!("Unable to join crossbeam scope")))?;

    return Ok(());
}

#[allow(dead_code)]
fn profiling(path: &str) -> Result<()> {

    let config = BenchmarkConfig::new(path)
        .context("Unable to create benchmark config")?;

    let batch_size = config.batch_sizes[0];
    let storage_size = batch_size * 10;
    let nb_cores = config.nb_cores[0];
    let conflict_rate = config.conflict_rates[0];

    let mut rng = match config.seed {
        Some(seed) => {
            StdRng::seed_from_u64(seed)
        },
        None => StdRng::seed_from_u64(rand::random())
    };

    let mut _initial_batch: Vec<Transaction> = batch_with_conflicts_new_impl(
        storage_size,
        batch_size,
        conflict_rate,
        &mut rng
    );
    // let mut initial_batch: Vec<Transaction> = batch_with_conflicts(
    //     batch_size,
    //     conflict_rate,
    //     &mut rng
    // );
    let mut backlog: Vec<Transaction> = Vec::with_capacity(_initial_batch.len());

    let reduced_vm_size = storage_size;
    // let reduced_vm_size = storage_size >> 1; // 50%       = 65536
    // let reduced_vm_size = storage_size >> 2; // 25%       = 32768
    // let reduced_vm_size = storage_size >> 3; // 12.5%     = 16384
    // let reduced_vm_size = storage_size >> 4; // 6.25%     = 8192
    // let reduced_vm_size = storage_size >> 5; // 3...%     = 4096
    // let reduced_vm_size = storage_size >> 6; // 1.5...%   = 2048
    // let reduced_vm_size = storage_size >> 7; // 0.7...%   = 1024

    // let mut s = DefaultHasher::new();
    let mut address_to_worker = vec![UNASSIGNED; reduced_vm_size];
    // let mut address_to_worker = HashMap::new();

    let mut storage = VmStorage::new(storage_size);
    let mut results: Vec<ExecutionResult> = vec!();

    let mut worker_to_tx: Vec<Vec<usize>> = vec![
        Vec::with_capacity(_initial_batch.len()/nb_cores as usize); nb_cores
    ];
    let mut next = vec![usize::MAX; _initial_batch.len()];

    let mut latency_assign = Duration::from_nanos(0);
    let mut latency_exec = Duration::from_nanos(0);

    let mut latency_assign_new_impl = Duration::from_nanos(0);
    let mut latency_exec_new_impl = Duration::from_nanos(0);

    let mut latency_assign_new_impl_2 = Duration::from_nanos(0);
    let mut latency_exec_new_impl_2 = Duration::from_nanos(0);

    // address_to_worker.fill(UNASSIGNED);
    // let assignment_original = assign_workers(
    //     nb_cores,
    //     &initial_batch,
    //     &mut address_to_worker,
    //     &mut backlog,
    //     // &mut worker_to_tx
    //     // &mut s
    // );
    // address_to_worker.fill(UNASSIGNED);
    // let _assignment = assign_workers_new_impl(
    //     nb_cores,
    //     &initial_batch,
    //     &mut address_to_worker,
    //     &mut backlog,
    //     &mut worker_to_tx
    //     // &mut s
    // );
    // address_to_worker.fill(UNASSIGNED);
    // let assignment_new_impl_2 = assign_workers_new_impl_2(
    //     nb_cores,
    //     &initial_batch,
    //     &mut address_to_worker,
    //     &mut backlog,
    //     &mut next
    //     // &mut s
    // );

    let total = Instant::now();
    for _i in 0..config.repetitions {
        // Reset variables -------------------------------------------------------------------------
        address_to_worker.fill(UNASSIGNED);
        storage.set_storage(200);
        results.truncate(0);
        backlog.truncate(0);
        let mut batch = _initial_batch.clone();
        // tx_to_worker = list of worker index the size of the main storage
        // Measure assign_workers
        let a = Instant::now();
        let assignment_original = assign_workers(
            nb_cores,
            &batch,
            &mut address_to_worker,
            &mut backlog,
            // &mut worker_to_tx
            // &mut s
        );
        latency_assign = latency_assign.add(a.elapsed());

        // Measure parallel execution
        let b =  Instant::now();
        WorkerC::crossbeam(
            nb_cores,
            &mut results,
            &mut batch,
            &mut backlog,
            &mut storage,
            &assignment_original,
        )?;
        latency_exec = latency_exec.add(b.elapsed());

        // Reset variables ------------------------------------------------------------------------
        // assignment contains lists of tx index, one for each worker
        address_to_worker.fill(UNASSIGNED);
        for m in worker_to_tx.iter_mut() {
            m.truncate(0);
        }
        results.truncate(0);
        backlog.truncate(0);
        storage.set_storage(200);
        let mut batch = _initial_batch.clone();
        // Measure assign_workers
        let a = Instant::now();
        let _assignment = assign_workers_new_impl(
            nb_cores,
            &batch,
            &mut address_to_worker,
            &mut backlog,
            &mut worker_to_tx
            // &mut s
        );
        latency_assign_new_impl = latency_assign_new_impl.add(a.elapsed());

        // Measure parallel execution
        let b =  Instant::now();
        WorkerC::crossbeam_new_impl(
            nb_cores,
            &mut results,
            &mut batch,
            &mut backlog,
            &mut storage,
            &worker_to_tx,
        )?;
        latency_exec_new_impl = latency_exec_new_impl.add(b.elapsed());

        // Reset variables -------------------------------------------------------------------------
        // next is a linked list of tx_indexes, that each worker is responsible for
        // head contains the first tx each worker is responsible for
        address_to_worker.fill(UNASSIGNED);
        next.fill(usize::MAX);
        results.truncate(0);
        backlog.truncate(0);
        storage.set_storage(200);
        let mut batch = _initial_batch.clone();
        // Measure assign_workers
        let a = Instant::now();
        let assignment_new_impl_2 = assign_workers_new_impl_2(
            nb_cores,
            &batch,
            &mut address_to_worker,
            &mut backlog,
            &mut next
            // &mut s
        );
        latency_assign_new_impl_2 = latency_assign_new_impl_2.add(a.elapsed());

        // Measure parallel execution
        let b =  Instant::now();
        WorkerC::crossbeam_new_impl_2(
            nb_cores,
            &mut results,
            &mut batch,
            &mut backlog,
            &mut storage,
            &next,
            &assignment_new_impl_2
        )?;
        latency_exec_new_impl_2 = latency_exec_new_impl_2.add(b.elapsed());

        // println!("Amount per address after exec: {}", storage.total()/storage_size as u64);
    }
    println!("Profiling took {:?}", total.elapsed());
    println!();
    println!("original:");
    println!("Average latency (assign): {:?}", latency_assign.div(config.repetitions as u32));
    println!("Average latency (exec): {:?}", latency_exec.div(config.repetitions as u32));
    let avg = latency_assign.add(latency_exec).div(config.repetitions as u32);
    println!("Together: {:.3?}", avg);
    println!("Throughput = {} tx/µs", batch_size as u128/avg.as_micros());
    println!();

    println!("new impl:");
    println!("Average latency (assign): {:?}", latency_assign_new_impl.div(config.repetitions as u32));
    println!("Average latency (exec): {:?}", latency_exec_new_impl.div(config.repetitions as u32));
    let avg = latency_assign_new_impl.add(latency_exec_new_impl).div(config.repetitions as u32);
    println!("Together: {:.3?}", avg);
    println!("Throughput = {} tx/µs", batch_size as u128/avg.as_micros());
    println!();

    println!("new impl 2:");
    println!("Average latency (assign): {:?}", latency_assign_new_impl_2.div(config.repetitions as u32));
    println!("Average latency (exec): {:?}", latency_exec_new_impl_2.div(config.repetitions as u32));
    let avg = latency_assign_new_impl_2.add(latency_exec_new_impl_2).div(config.repetitions as u32);
    println!("Together: {:.3?}", avg);
    println!("Throughput = {} tx/µs", batch_size as u128/avg.as_micros());
    println!();

    // println!("Total {} runs: {:?} (assign)", config.repetitions, latency_sum);
    // println!("Average latency (assign): {:?}", latency_sum.div(config.repetitions as u32));
    //
    // println!();
    // println!("Total {} runs: {:?} (exec)", config.repetitions, exec_latency_sum);
    // println!("Average latency (exec): {:?}", exec_latency_sum.div(config.repetitions as u32));
    // println!("See you, world!");

    Ok(())
}