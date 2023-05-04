#![allow(unused_variables)]
#![allow(unused_imports)]
#![allow(unused_mut)]
#![allow(dead_code)]
#![allow(unreachable_code)]
extern crate anyhow;
extern crate either;
extern crate tokio;


use futures::future::BoxFuture;
use itertools::Itertools;
use voracious_radix_sort::RadixSort;
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::{fs, mem};
use std::ops::{Add, Div, Mul, Sub};
use std::sync::mpsc::{channel, Receiver};
use std::sync::{Arc, Mutex};
use std::thread::sleep;
use tokio::time::{Duration, Instant};
use std::time::Instant as StdInstant;

use thincollections::thin_set::ThinSet;
use nohash_hasher::{BuildNoHashHasher, IntSet};
use rayon::prelude::*;
use anyhow::{anyhow, Context, Result};
use bloomfilter::Bloom;
use futures::SinkExt;
use futures::task::SpawnExt;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use serde::Serialize;
use strum::IntoEnumIterator;
use tinyset::SetU64;
use tokio::task::JoinHandle;

use testbench::benchmark::{benchmarking, TestBench};
use testbench::config::{BenchmarkConfig, ConfigFile, RunParameter};
use testbench::{bounded_array, contract, debug, debugging, utils};
use testbench::applications::{Currency, Workload};
use testbench::contract::{AtomicFunction, FunctionParameter, SenderAddress, StaticAddress, TestSharedMap, Transaction};
use testbench::transaction::{Transaction as BasicTransaction, TransactionAddress};
use testbench::utils::{batch_with_conflicts, batch_with_conflicts_new_impl, BoundedArray};
use testbench::vm::{ExecutionResult, Executor};
use testbench::vm_a::VMa;
use testbench::vm_c::VMc;
use testbench::vm_utils::{assign_workers, UNASSIGNED, VmStorage, VmType};
use testbench::wip::{assign_workers_new_impl, assign_workers_new_impl_2, BackgroundVM, BackgroundVMDeque, ConcurrentVM, Param, Word};
use testbench::contract::FunctionResult::Another;
use testbench::parallel_vm::{ParallelVM, ParallelVmCollect, ParallelVmImmediate};
use testbench::sequential_vm::SequentialVM;
use testbench::worker_implementation::WorkerC;
use std::str::FromStr;
use core_affinity::CoreId;
use futures::stream::iter;
use testbench::micro_benchmark::{all_numa_latencies};

type A = u64;
type Set = IntSet<u64>;
#[derive(Copy, Clone, Debug)]
struct T {
    from: A,
    to: A,
}

#[tokio::main]
async fn main() -> Result<()> {

    let total = Instant::now();

    // TestSharedMap::test_all();

    // tokio::task::spawn_blocking(|| {
    //     // println!("Previous version");
    //     // benchmarking("benchmark_config.json");
    //     //
    //     // println!();
    //     // println!("New version");
    //     TestBench::benchmark("benchmark_config.json")
    // }).await.expect("Task panicked")?;

    let from_power = 2;
    // let to_power = 18;
    let to_power = 5;

    // match numa_latency_between(0, 0, from_power, to_power).await {
    //     Ok(_) => {},
    //     Err(e) => eprintln!("Failed to run micro benchmark: {:?}", e)
    // }

    match all_numa_latencies(4, from_power, to_power).await {
        Ok(_) => {},
        Err(e) => eprintln!("Failed to run micro benchmark: {:?}", e)
    }

    // let param = BenchmarkConfig{
    //     vm_types: vec![VmType::A],
    //     nb_schedulers: vec![0],
    //     nb_executors: vec![0],
    //     batch_sizes: vec![0],
    //     workloads: vec![Workload::TransferPiece(0.0), Workload::Transfer(0.0)],
    //     repetitions: 1,
    //     warmup: 1,
    //     seed: Some(1),
    // };
    //
    // param.save("config_test.json")?;

    // let workload = Workload::from_str("Transfer(0.0)").unwrap();
    // let workload = Workload::from_str("Transfer").unwrap();
    // println!("{:?}", workload);
    // println!("{:?}, {:?}", Workload::Transfer(0.0), Workload::TransferPiece(0.0));

    // let param = BenchmarkConfig::new("config_test.json")?;
    // println!("{:?}", param);

    // #[derive(Serialize)]
    // struct TupleStruct(i32, i32);
    // let param = TupleStruct(0, 1);
    //
    // let str = serde_json::to_string_pretty(&param)
    //     .context(format!("Unable to create json of {}", "config_test.json"))?;
    //
    // fs::write("benchmark_config.json", str)
    //     .context(format!("Unable to write {} to file", "config_test.json"))?;

    // manual_test()?;

    // profiling("benchmark_config.json")?; // ???
    // wip_main().await;

    // println!("\nComparing chunk schedule and exec: ==========================");
    // println!();
    // profile_schedule_chunk(batch.clone(), 100, 8, 8);  // ???

    // println!("main took {:?}", total.elapsed());

    Ok(())
}

fn manual_test() -> Result<()> {
    let mut rng = StdRng::seed_from_u64(10);
    let batch_size = 65536;
    let storage_size = 100 * batch_size;

    let nb_schedulers = 8;
    let nb_workers = 1;

    let iter = 100;

    let a = Instant::now();
    let batch: Vec<Transaction<2, 2>> = Currency::transfers_workload(
        storage_size,
        batch_size,
        0.0,
        &mut rng
    );
    println!("Creating batch of size {} took {:?}", batch_size, a.elapsed());

    let a = Instant::now();
    let mut concurrent = ConcurrentVM::new(storage_size, nb_schedulers, nb_workers)?;
    concurrent.storage.set_storage(20 * iter);

    let mut parallel_collect = ParallelVmCollect::new(storage_size, nb_schedulers, nb_workers)?;
    parallel_collect.vm.storage.set_storage(20 * iter);

    let mut parallel_immediate = ParallelVmImmediate::new(storage_size, nb_schedulers, nb_workers)?;
    parallel_immediate.vm.storage.set_storage(20 * iter);

    println!("Creating vms took {:?}", a.elapsed());

    // let mut concurrent_latency_v3 = Vec::with_capacity(iter as usize);
    // let mut concurrent_latency_v5 = Vec::with_capacity(iter as usize);

    // for _ in 0..iter {
    //     let b = batch.clone();
    //     let start = Instant::now();
    //     let _ = concurrent.execute_variant_3(b)?;
    //     concurrent_latency_v3.push(start.elapsed());
    //
    //     let b = batch.clone();
    //     let start = Instant::now();
    //     let _ = concurrent.execute_variant_5(b)?;
    //     concurrent_latency_v5.push(start.elapsed());
    // }
    // println!("Concurrent v3:");
    // println!("\tAverage latency = {}", utils::mean_ci_str(&concurrent_latency_v3));
    // println!();
    //
    // println!("Concurrent v5:");
    // println!("\tAverage latency = {}", utils::mean_ci_str(&concurrent_latency_v5));
    // println!();

    println!("====================");

    let mut parallel_latency_v6 = Vec::with_capacity(iter as usize);
    let mut parallel_scheduling_v6 = Vec::with_capacity(iter as usize);
    let mut parallel_execution_v6 = Vec::with_capacity(iter as usize);

    let mut parallel_latency_v7 = Vec::with_capacity(iter as usize);
    let mut parallel_scheduling_v7 = Vec::with_capacity(iter as usize);
    let mut parallel_execution_v7 = Vec::with_capacity(iter as usize);

    for _ in 0..iter {
        let b = batch.clone();
        let (scheduling, execution) = parallel_collect.execute(b)?;

        let b = batch.clone();
        let (scheduling, execution) = parallel_immediate.execute(b)?;
    }
    parallel_collect.vm.storage.set_storage(20 * iter);
    parallel_immediate.vm.storage.set_storage(20 * iter);

    for _ in 0..iter {
        let b = batch.clone();
        let start = Instant::now();
        let (scheduling, execution) = parallel_collect.execute(b)?;
        parallel_latency_v6.push(start.elapsed());
        parallel_scheduling_v6.push(scheduling);
        parallel_execution_v6.push(execution);

        let b = batch.clone();
        let start = Instant::now();
        let (scheduling, execution) = parallel_immediate.execute(b)?;
        parallel_latency_v7.push(start.elapsed());
        parallel_scheduling_v7.push(scheduling);
        parallel_execution_v7.push(execution);
    }
    println!("ParallelCollect (v6):");
    println!("\tAverage latency = {}", utils::mean_ci_str(&parallel_latency_v6));
    println!("\tAvg scheduling latency = {}", utils::mean_ci_str(&parallel_scheduling_v6));
    println!("\tAvg execution latency = {}", utils::mean_ci_str(&parallel_execution_v6));
    println!();

    println!("ParallelImmediate (v7):");
    println!("\tAverage latency = {}", utils::mean_ci_str(&parallel_latency_v7));
    println!("\tAvg scheduling latency = {}", utils::mean_ci_str(&parallel_scheduling_v7));
    println!("\tAvg execution latency = {}", utils::mean_ci_str(&parallel_execution_v7));
    println!();


    /* TODO To be able to have split transfers, we need to have tx pieces with different number of addresses.
        or use options to prevent the schedulers to think a transaction conflict with itself
        /!\ Even adding a usize inside the Transaction struct makes things slower...

     */
    // println!("========= Testing Transaction pieces ===========");
    // let mut rng = StdRng::seed_from_u64(10);
    // let a = Instant::now();
    // let batch_pieces: Vec<Transaction> = Currency::split_transfers_workload(
    //     storage_size,
    //     batch_size,
    //     0.0,
    //     &mut rng
    // );
    // println!("Creating batch of size {} took {:?}", batch_size, a.elapsed());
    // let mut parallel_latency_v6 = Vec::with_capacity(iter as usize);
    // let mut parallel_scheduling_v6 = Vec::with_capacity(iter as usize);
    // let mut parallel_execution_v6 = Vec::with_capacity(iter as usize);
    //
    // let mut parallel_latency_v7 = Vec::with_capacity(iter as usize);
    // let mut parallel_scheduling_v7 = Vec::with_capacity(iter as usize);
    // let mut parallel_execution_v7 = Vec::with_capacity(iter as usize);
    // for _ in 0..iter {
    //     let b = batch_pieces.clone();
    //     let start = Instant::now();
    //     let (scheduling, execution) = parallel_collect.execute(b)?;
    //     parallel_latency_v6.push(start.elapsed());
    //     parallel_scheduling_v6.push(scheduling);
    //     parallel_execution_v6.push(execution);
    //
    //     let b = batch_pieces.clone();
    //     let start = Instant::now();
    //     let (scheduling, execution) = parallel_immediate.execute(b)?;
    //     parallel_latency_v7.push(start.elapsed());
    //     parallel_scheduling_v7.push(scheduling);
    //     parallel_execution_v7.push(execution);
    // }
    // println!("Parallel v6 (split transfer):");
    // println!("\tAverage latency = {}", utils::mean_ci_str(&parallel_latency_v6));
    // println!("\tAvg scheduling latency = {}", utils::mean_ci_str(&parallel_scheduling_v6));
    // println!("\tAvg execution latency = {}", utils::mean_ci_str(&parallel_execution_v6));
    // println!();
    //
    // println!("Parallel v7 (split transfer):");
    // println!("\tAverage latency = {}", utils::mean_ci_str(&parallel_latency_v7));
    // println!("\tAvg scheduling latency = {}", utils::mean_ci_str(&parallel_scheduling_v7));
    // println!("\tAvg execution latency = {}", utils::mean_ci_str(&parallel_execution_v7));
    // println!();

    Ok(())
}

async fn wip_main() -> Result<()>{
    println!("Hello, world!");
    let total = Instant::now();

    // let _ = BasicWorkload::run(config, 1).await;
    // let _ = ContentionWorkload::run(config, 1).await;
    // let _ = TransactionLoop::run(config, 1).await;
    // let _ = ConflictWorkload::run(config, 1);

    // benchmarking("benchmark_config.json")?;
    // profiling("benchmark_config.json")?; // ???

    // profile_old_tx("benchmark_config.json")?;
    // profile_new_tx("benchmark_config.json")?;
    // profile_new_contract("benchmark_config.json")?;

    // let res: Vec<_> = (0..4).into_par_iter().map(|delay| {
    //     sleep(Duration::from_secs(delay));
    //     eprintln!("waited {}s", delay);
    //     delay
    // }).collect();
    // for delay in res.iter() {
    //     eprintln!("collected {}", delay);
    // }
    // (0..4).into_par_iter().map(|delay| {
    //     sleep(Duration::from_secs(delay));
    //     eprintln!("waited {}s", delay);
    //     delay
    // }).collect::<Vec<u64>>().iter().for_each(|delay| {
    //     eprintln!("collected {}", delay);
    // });

    // let res: () = (0..10).into_par_iter().map(|delay| {
    //     sleep(Duration::from_secs(1));
    //     eprintln!("waited {}s", delay);
    //     delay
    // }).fold(|| (), |acc, delay| {
    //     eprintln!("collected {}", delay);
    //     acc
    // }).collect();
// https://morestina.net/blog/1432/parallel-stream-processing-with-rayon

    let mut rng = StdRng::seed_from_u64(10);
    let batch_size = 65536;
    let storage_size = 100 * batch_size;

    // print!("Creating batch of size {}... ", batch_size);
    let a = Instant::now();
    let batch: Vec<T> = batch_with_conflicts_new_impl(
        storage_size,
        batch_size,
        0.0,    // TODO
        &mut rng
    ).par_iter().map(|tx| T { from: tx.from, to: tx.to}).collect();
    println!("Took {:?}", a.elapsed());

    let nb_schedulers = 8;
    let nb_executors = 1;
    // println!();
    //
    // let small_batch_size = 2048;
    // let small_storage_size = 100 * batch_size;
    // print!("Creating batch of size {}... ", small_batch_size);
    // let a = Instant::now();
    // let small_batch: Vec<T> = batch_with_conflicts_new_impl(
    //     small_storage_size,
    //     small_batch_size,
    //     0.0,
    //     &mut rng
    // ).par_iter().map(|tx| T { from: tx.from, to: tx.to }).collect();
    //
    // println!("Took {:?}", a.elapsed());
    // println!();

    // =================================================
    // let (test_batch, test_storage) = (small_batch.clone(), small_storage_size);
    let (test_batch, test_storage) = (batch.clone(), storage_size);
    let iter = 1;

    let xyz = Instant::now();
    let new_batch: Vec<_> = test_batch.par_iter()
        .enumerate()
        .map(|(tx_index, tx)| Transaction {
            sender: tx.from as SenderAddress,
            function: AtomicFunction::Transfer,
            // nb_addresses: 2,
            addresses: [tx.from as StaticAddress, tx.to as StaticAddress],
            params: [2, tx_index as FunctionParameter]
        }).collect();
    println!("Mapping took {:?}", xyz.elapsed());

    // ???
    let mut concurrent = ConcurrentVM::new(
        test_storage,
        nb_schedulers,
        nb_executors)?;

    let mut background = BackgroundVM::new(
        test_storage,
        nb_schedulers,
        nb_executors)?;

    let mut parallel = ParallelVmImmediate::new(
        test_storage,
        nb_schedulers,
        nb_executors)?;

    let mut wtf = ParallelVmCollect::new(
        test_storage,
        nb_schedulers,
        nb_executors)?;

    // background.stop().await;

    let mut duration = Duration::from_nanos(0);
    concurrent.storage.set_storage(20 * iter);
    background.storage.set_storage(20 * iter);
    parallel.vm.storage.set_storage(20 * iter);
    wtf.vm.storage.set_storage(20 * iter);

    println!("Testing different variants: ==========================");
    println!();
    // ???
    // Multiple variants that are promising (need to test on AWS)
    for _ in 0..iter {
        let b = new_batch.clone();
        // let s = Instant::now();
        // let _ = concurrent.execute(b);
        // duration = duration.add(s.elapsed());
        //

        // println!("Variant 6 cleaned ---------------------------------");
        // let b = new_batch.clone();
        // let start = Instant::now();
        // let err = parallel.execute(b);
        // println!("Overall: {:?}", start.elapsed());
        // // println!("Err? {:?}", err);
        // println!();
        //
        // println!("Variant 6 WTF ---------------------------------");
        // let b = new_batch.clone();
        // let start = Instant::now();
        // let err = wtf.execute(b);
        // println!("Overall: {:?}", start.elapsed());
        // println!("Err? {:?}", err.unwrap().0);
        // println!();

        println!("background ---------------------------------");
        let b = new_batch.clone();
        let start = Instant::now();
        let err = background.execute(b).await;
        // println!("Err? {:?}", err);
        println!("Background took {:?}", start.elapsed());
        println!();
        //
        println!("Variant 1 async ---------------------------------");
        let b = new_batch.clone();
        let start = Instant::now();
        let err = concurrent.execute_variant_1_async(b).await;
        println!("Overall: {:?}", start.elapsed());
        // println!("Err? {:?}", err);
        println!();
        //
        println!("Variant 1 ---------------------------------");
        let b = new_batch.clone();
        let start = Instant::now();
        let err = concurrent.execute_variant_1(b);
        println!("Overall: {:?}", start.elapsed());
        // println!("Err? {:?}", err);
        println!();

        println!("Variant 2 ---------------------------------");
        let b = new_batch.clone();
        let start = Instant::now();
        let err = concurrent.execute_variant_2(b);
        println!("Overall: {:?}", start.elapsed());
        // println!("Err? {:?}", err);
        println!();

        println!("Variant 3 ---------------------------------");
        let b = new_batch.clone();
        let start = Instant::now();
        let err = concurrent.execute_variant_3(b);
        println!("Overall: {:?}", start.elapsed());
        // println!("Err? {:?}", err);
        println!();

        println!("Variant 4 ---------------------------------");
        let b = new_batch.clone();
        let start = Instant::now();
        let err = concurrent.execute_variant_4(b);
        println!("Overall: {:?}", start.elapsed());
        // println!("Err? {:?}", err);
        println!();

        println!("Variant 5 ---------------------------------");
        let b = new_batch.clone();
        let start = Instant::now();
        let err = concurrent.execute_variant_5(b);
        println!("Overall: {:?}", start.elapsed());
        // println!("Err? {:?}", err);
        println!();

        println!("Variant 6 ---------------------------------");
        let b = new_batch.clone();
        let start = Instant::now();
        let err = concurrent.execute_variant_6(b);
        println!("Overall: {:?}", start.elapsed());
        // println!("Err? {:?}", err);
        println!();
        //
        println!("Variant 7 ---------------------------------");
        let b = new_batch.clone();
        let start = Instant::now();
        let _ = concurrent.execute_variant_7(b);
        println!("Overall: {:?}", start.elapsed());
        println!();

        // println!();
    }
    // println!();
    // println!("Concurrent:");
    // println!("\t{} iterations took {:?}", iter, duration);
    // println!("\tAverage latency = {:?}", duration.div(iter as u32));
    // println!();

    let mut sequential = SequentialVM::new(test_storage)?;
    let mut duration = Duration::from_nanos(0);
    sequential.storage.fill(20 * iter);

    for _ in 0..iter {
        let b = new_batch.clone();
        let s = Instant::now();
        let _ = sequential.execute(b);
        duration = duration.add(s.elapsed());
    }
    println!("Sequential:");
    println!("\t{} iterations took {:?}", iter, duration);
    println!("\tAverage latency = {:?}", duration.div(iter as u32));
    println!();

    let mut duration = Duration::from_nanos(0);
    sequential.storage.fill(20 * iter);
    for _ in 0..iter {
        let b = new_batch.clone();
        let s = Instant::now();
        let _res = sequential.execute_with_results(b);
        // println!("{:?}", _res.unwrap().into_iter().take(10).collect::<Vec<contract::FunctionResult>>());
        duration = duration.add(s.elapsed());
    }

    println!("Sequential with result:");
    println!("\t{} iterations took {:?}", iter, duration);
    println!("\tAverage latency = {:?}", duration.div(iter as u32));
    println!();
    background.stop().await;


    // println!("\nComparing chunk schedule and exec: ==========================");
    // println!();
    // profile_schedule_chunk(new_batch.clone(), 100, 8, 8);  // ???
    // // => need to split batch in 8 for the scheduling to be fast enough
    // // => executing one 8th takes the same time in parallel and sequentially?

    // println!("\nTesting scheduling sequential vs parallel (65536 tx): ==========================");
    // println!();
    // profile_schedule_backlog_single_pass(new_batch.clone(), 1, 8).await;    // ???
    // // // => scheduling a chunk is slower in parallel?!
    // // // This is caused by threads not being equivalent to cores
    //
    // println!("\nTesting other method (nested task): ==========================");
    // println!();
    // try_other_method(new_batch.clone(), 1, 8, true, 1).await;    // ???

    // test_profile_schedule_backlog_single_pass(new_batch.clone(), 1).await;
    // check_reallocation_overhead(); // ???

    // println!("=====================================================");
    // println!();
    // try_merge_sort(&batch, storage_size).await;

    return Ok(());
    // profile_rayon_latency();

    let mut b = new_batch.clone();
    let a = Instant::now();
    let nb_tasks = 3;
    // let tmp: Vec<_> = (0..nb_tasks).map(|_| {
    //      tokio::spawn(async move {
    //          // let v: Vec<usize> = vec!();
    //          let mut v: Vec<usize> = Vec::with_capacity(8000);
    //          v.push(25);
    //         sleep(Duration::from_micros(300));
    //     })
    // }).collect();
    // for h in tmp {
    //     let _ = h.await;
    // }


    let nb_schedulers = 8;
    let chunk_size = b.len()/nb_schedulers + 1;
    let mut handles: Vec<JoinHandle<Duration>> = Vec::with_capacity(nb_schedulers);
    // for scheduler in 0..nb_schedulers {
    //     let mut scheduled: Box<Vec<ContractTransaction>> = Box::new(Vec::with_capacity(chunk_size));
    //     let mut postponed: Box<Vec<ContractTransaction>> = Box::new(Vec::with_capacity(chunk_size));
    //     let mut working_set = Box::new(SetU64::with_capacity_and_max(
    //         2 * 65536,
    //         100 * 65536 as u64
    //     ));
    //
    //     handles.push(tokio::spawn(async move {
    //         sleep(Duration::from_micros(300));
    //         scheduled.len();
    //         postponed.len();
    //         working_set.len();
    //     }));
    // }

    let mut data = Vec::with_capacity(nb_schedulers);
    let chunks: Vec<Vec<Transaction<2, 2>>> = b
        .into_iter()
        .chunks(chunk_size)
        .into_iter()
        .map(|chunk| chunk.collect())
        .collect();
    for chunk in chunks {
        let mut scheduled: Vec<Transaction<2, 2>> = Vec::with_capacity(chunk_size);
        let mut postponed: Vec<Transaction<2, 2>> = Vec::with_capacity(chunk_size);
        let mut working_set = SetU64::with_capacity_and_max(
            2 * 32,
            100 * 65536 as u64
        );
        // let working_set = Vec::with_capacity(2 * 65536);
        // let working_set = HashSet::new();

        data.push((chunk, scheduled, postponed, working_set));
    }

    type ClosureInput = (Vec<Transaction<2, 2>>, Vec<Transaction<2, 2>>, Vec<Transaction<2, 2>>, SetU64);
    // type ClosureInput = (Vec<ContractTransaction>, Vec<ContractTransaction>, Vec<ContractTransaction>, Vec<StaticAddress>);
    // type ClosureInput = (Vec<ContractTransaction>, Vec<ContractTransaction>, Vec<ContractTransaction>, HashSet<StaticAddress>);
    let a = Instant::now();
    let computation_1 = |(chunk, mut scheduled, mut postponed, mut working_set): ClosureInput| {
        // 56 micro
    };
    let computation_2 = |(chunk, mut scheduled, mut postponed, mut working_set): ClosureInput| {
        // 200 micro
        chunk.len();
        scheduled.len();
        postponed.len();
        let _ = working_set.len();
    };
    let computation_3 = |(chunk, mut scheduled, mut postponed, mut working_set): ClosureInput| {
        sleep(Duration::from_micros(700));
    };
    let computation_4 = |(chunk, mut scheduled, mut postponed, mut working_set): ClosureInput| {
        // 800 micro
        chunk.len();
        scheduled.len();
        postponed.len();
        let _ = working_set.len();
        sleep(Duration::from_micros(700));
    };
    let computation_5 = |(chunk, mut scheduled, mut postponed, mut working_set): ClosureInput| {
        // 2.2 ms
        let mut multiple = 1;
        'outer: for tx in chunk {
            for addr in tx.addresses.iter() {
                if working_set.contains(*addr as u64) {
                    // Can't add tx to schedule
                    postponed.push(tx.clone());
                    continue 'outer;
                }
                // if working_set.contains(addr) {
                //     // Can't add tx to schedule
                //     postponed.push(tx.clone());
                //     continue 'outer;
                // }
                // if let Ok(_) = working_set.binary_search(addr) {
                //     // Can't add tx to schedule
                //     postponed.push(tx.clone());
                //     continue 'outer;
                // }
                // if working_set.contains(addr) {
                //     // Can't add tx to schedule
                //     postponed.push(tx.clone());
                //     continue 'outer;
                // }
            }

            // Can add tx to schedule
            scheduled.push(tx.clone());
            for addr in tx.addresses.iter() {
                working_set.insert(*addr as u64);
                // working_set.push(*addr);
                // match working_set.binary_search(addr) {
                //     Ok(pos) => {} // element already in vector @ `pos`
                //     Err(pos) => working_set.insert(pos, *addr),
                // }
                // working_set.insert(*addr);
            }
            // working_set.extend_from_slice(&tx.addresses)

            if scheduled.len() > 32 * multiple {
                let _ = working_set.drain();
                // working_set = SetU64::with_capacity_and_max(
                //     2 * 32,
                //     100 * 65536 as u64
                // );
            }
        }
    };

    let computation_test = |(chunk, mut scheduled, mut postponed, mut working_set): ClosureInput| {
        // 800 micro
        chunk.len();
        // scheduled.push(chunk[0]);
        // scheduled.extend(chunk.iter());
        scheduled.len();
        // postponed.push(chunk[1]);
        // postponed.extend(chunk.iter());
        postponed.len();
        // working_set.insert(chunk[2].addresses[0] as u64);
        for tx in chunk.iter() {
            for addr in tx.addresses.iter() {
                working_set.insert(*addr as u64);
            }
        }
        let _ = working_set.len();
        sleep(Duration::from_micros(700));
    };

    let mut index = 0;
    for (chunk, mut scheduled, mut postponed, mut working_set) in data {
        // let computation = computation_1;    // 70 micro
        // let computation = computation_2;    // 100 micro
        // let computation = computation_3;
        // let computation = computation_4;    // 600 micro
        let computation = computation_5;    // 2 ms
        // let computation = computation_test;

        handles.push(tokio::spawn(async move {
            let parallel_exec_latency = Instant::now();
            computation((chunk, scheduled, postponed, working_set));
            parallel_exec_latency.elapsed()
        }));

        // let sequential_latency = Instant::now();
        // computation((chunk, scheduled, postponed, working_set));
        // println!("Scheduler {} took {:?}", index, sequential_latency.elapsed());
        // index += 1;
    }

    for (index, scheduler) in handles.into_iter().enumerate() {
        let latency = scheduler.await;
        println!("Scheduler {} took {:?}", index, latency.unwrap());
    }

    // let _ = crossbeam::scope(|s| {
    //     let tmp: Vec<_> = (0..nb_tasks).map(|_| {
    //         s.spawn(|s| {
    //             sleep(Duration::from_micros(300));
    //         })
    //     }).collect();
    //     for h in tmp {
    //         let _ = h.join();
    //     }
    // });

    // let x = tokio::spawn(async move {
    //     sleep(Duration::from_micros(300));
    // });
    // let y = tokio::spawn(async move {
    //     sleep(Duration::from_micros(300));
    // });
    // let z = tokio::spawn(async move {
    //     sleep(Duration::from_micros(300));
    // });
    // let _ = x.await;
    // let _ = y.await;
    // let _ = z.await;
    println!("Spawn + join took {:?}", a.elapsed());


    // println!("=====================================================");
    // println!();
    // try_int_set(&batch, &mut rng, storage_size);
    // try_tiny_set(&batch, &mut rng, storage_size);

    // println!("=====================================================");
    // println!();
    // try_sorting(&batch);

    // println!("=====================================================");
    // println!();
    // // try_sequential(&small_batch, &mut rng, small_storage_size);
    // let mut sum = Duration::from_nanos(0);
    // let nb_reps = 10;
    // for _ in 0..nb_reps {
    //     sum = try_parallel_mutex(&batch, &mut rng, storage_size).add(sum);
    //     // sum = try_parallel_unsafe(&small_batch, &mut rng, storage_size).add(sum);
    //     // sum = try_parallel_without_mutex(&small_batch, &mut rng, small_storage_size).add(sum);
    // }
    // println!("Avg latency = {:?}", sum.div(nb_reps));

    // println!("=====================================================");
    // println!();
    //
    // try_scheduling_sequential(&batch, storage_size);

    // println!("=====================================================");
    // println!();
    // try_scheduling_parallel(&batch, storage_size);

    // println!("=====================================================");
    // println!();
    // let start = Instant::now();
    // let set_capacity = batch.len() * 2;
    // // let set_capacity = 65536 * 2;
    // let mut rounds = vec![(
    //     tinyset::SetU64::with_capacity_and_max(set_capacity, storage_size as u64),
    //     vec!()
    // )];
    // 'outer: for tx in batch.iter() {
    //     for (round_addr, round_tx) in rounds.iter_mut() {
    //         if !round_addr.contains(tx.from) &&
    //             !round_addr.contains(tx.to) {
    //             round_addr.insert(tx.from);
    //             round_addr.insert(tx.to);
    //             round_tx.push(tx);
    //             continue 'outer;
    //         }
    //     }
    //     // Can't be added to any round
    //     let new_round_addr = tinyset::SetU64::with_capacity_and_max(set_capacity, storage_size as u64);
    //     let new_round_tx = vec!(tx);
    //     rounds.push((new_round_addr, new_round_tx));
    // }
    //
    // let elapsed = start.elapsed();
    // println!("Serial:");
    // println!("Need {} rounds", rounds.len());
    // println!("Took {:?};", elapsed);
    // rounds


    // profile_parallel_contract()?;
    println!("=====================================================");

    println!("Total took {:?}", total.elapsed());


    // test_new_transactions()?;
    // test_new_contracts()?;

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

fn check_reallocation_overhead() {

    let timer = std::time::Instant::now();
    let mut v1 = Vec::<(usize, usize, usize, usize)>::with_capacity(4);
    let mut v2 = Vec::<(usize, usize, usize, usize)>::with_capacity(4);
    for _ in 0..100000000 {
        let random_val1 = rand::random::<usize>();
        let random_val2 = rand::random::<usize>();
        v1.truncate(0);
        v1.push((random_val1, random_val2, random_val1, random_val2));
        v1.push((random_val2, random_val1, random_val2, random_val1));

        v2.truncate(0);
        v2.push((random_val2, random_val1, random_val2, random_val1));
        v2.push((random_val1, random_val2, random_val1, random_val2));

        if v1.len() != 2 && v1[0].0 != random_val1 {
            println!("v1 is empty");
        }
        if v2.len() != 2 && v2[0].0 != random_val2 {
            println!("v2 is empty");
        }
    }
    println!("Mut Vec: Completed in {:?}", timer.elapsed());

    let timer = std::time::Instant::now();
    for _ in 0..100000000 {
        let random_val1 = rand::random::<usize>();
        let random_val2 = rand::random::<usize>();
        let v1 = vec![
            (random_val1, random_val2, random_val1, random_val2),
            (random_val2, random_val1, random_val2, random_val1),
        ];
        let v2 = vec![
            (random_val2, random_val1, random_val2, random_val1),
            (random_val1, random_val2, random_val1, random_val2),
        ];

        if v1.len() != 2 && v1[0].0 != random_val1 {
            println!("v1 is empty");
        }
        if v2.len() != 2 && v2[0].0 != random_val2 {
            println!("v2 is empty");
        }
    }
    println!("New Vec: Completed in {:?}", timer.elapsed());

    let timer = std::time::Instant::now();
    for _ in 0..100000000 {
        let mut v1 = Vec::<(usize, usize, usize, usize)>::with_capacity(2);
        let mut v2 = Vec::<(usize, usize, usize, usize)>::with_capacity(2);

        let random_val1 = rand::random::<usize>();
        let random_val2 = rand::random::<usize>();
        v1.truncate(0);
        v1.push((random_val1, random_val2, random_val1, random_val2));
        v1.push((random_val2, random_val1, random_val2, random_val1));

        v2.truncate(0);
        v2.push((random_val2, random_val1, random_val2, random_val1));
        v2.push((random_val1, random_val2, random_val1, random_val2));

        if v1.len() != 2 && v1[0].0 != random_val1 {
            println!("v1 is empty");
        }
        if v2.len() != 2 && v2[0].0 != random_val2 {
            println!("v2 is empty");
        }
    }
    println!("Mut Vec (no macro): Completed in {:?}", timer.elapsed());

}

fn mock() {

    // Estimated from original assign_worker implementation
    let schedule_latency = Duration::from_nanos(2);
    let assign_worker_latency = |nb_tx| {
        Duration::from_micros(900).mul(nb_tx).div(65536)
    };

    println!("assign_worker latency for 65536 tx: {:?}", assign_worker_latency(65536));
    println!("assign_worker latency for 16384 tx: {:?}", assign_worker_latency(16384));

    // Estimated from serial vm implementation
    let tx_execution_latency = Duration::from_nanos(35);

    // let nb_tasks = 65536;
    // let nb_schedulers = 1;
    // let nb_workers = 4;
    // let prefix_size = 0;
    // let nb_chunks_per_scheduler = 1;
    //
    // let parallel_execution_overhead = 1;

    // // parameters corresponding to original implementation
    let nb_tasks = 65536;
    let nb_schedulers = 2;
    let nb_workers = 4;
    let prefix_size = 0;
    let nb_chunks_per_scheduler = 2;

    let parallel_execution_overhead = 1;

    //region Mock pipeline execution ===================================================================
    let mut tasks: Vec<usize> = (0..nb_tasks).collect();
    println!("Computing latency using pipeline:");
    let pipeline_start = Instant::now();

    let (prefix, suffix) = tasks.split_at(prefix_size);
    let (mut schedule_sender, schedule_receiver) = channel();

    let scheduler_backlog_size = suffix.len()/nb_schedulers;

    let tmp = suffix
        .par_chunks(scheduler_backlog_size)
        .enumerate()
        .map_with(
            schedule_sender,
            |sender, (scheduler, scheduler_backlog)| {

                if scheduler_backlog.len() >= nb_chunks_per_scheduler {
                    let mut chunk_size = scheduler_backlog.len()/nb_chunks_per_scheduler;

                    scheduler_backlog
                        .chunks(chunk_size)
                        .enumerate()
                        .for_each(|(chunk_index, chunk)| {
                            // let sleep_duration = schedule_latency.clone()
                            //     .mul(chunk.len() as u32);
                            let sleep_duration = assign_worker_latency(chunk.len() as u32);
                            sleep(sleep_duration);
                            if let Err(e) = sender.send((scheduler, chunk)) {
                                debug!("Scheduler {} failed to send its schedule", scheduler);
                            } else {
                                debug!("Scheduler {} sent the schedule for its {}-th chunk", scheduler, chunk_index);
                            }
                        });
                } else {
                    // Not enough backlog to split into chunks
                    // let sleep_duration = schedule_latency.clone()
                    //     .mul(scheduler_backlog.len() as u32);
                    let sleep_duration = assign_worker_latency(scheduler_backlog.len() as u32);
                    sleep(sleep_duration);
                    if let Err(e) = sender.send((scheduler, scheduler_backlog)) {
                        debug!("Scheduler {} failed to send its schedule", scheduler);
                    } else {
                        debug!("Scheduler {} sent the schedule for its only chunk", scheduler);
                    }
                }

                scheduler
            }
        );

    let main_thread = |receiver: Receiver<(usize, &[usize])>, prefix: &[usize]| {
        for i in prefix.iter() {
            sleep(tx_execution_latency);
            debug!("main thread executed {}-th tx sequentially", i);
        }

        let mut start_parallel = Instant::now();
        while let Ok((scheduler, chunk)) = receiver.recv() {
            let waited = start_parallel.elapsed();
            let exec_start = Instant::now();
            let jobs_per_worker = if chunk.len() < nb_workers {
                1
            } else {
                chunk.len().div(nb_workers) + 1
            };

            // Using rayon -------------------------------------------------------------------------
            let workers: Vec<()> = chunk.par_chunks(jobs_per_worker).map(|worker_jobs| {
                let sleep_duration = tx_execution_latency.clone()
                    .mul(parallel_execution_overhead * worker_jobs.len() as u32);
                sleep(sleep_duration);
            }).collect();
            let exec_duration = exec_start.elapsed();
            debug!("({:.2?}) worker pool executed {:?} tx from scheduler {}.",
                  waited, chunk.len(), scheduler);
            debug!("\tTook {:?} using {} workers", exec_duration, workers.len());

            // Using crossbeam ---------------------------------------------------------------------
            // let _ = crossbeam::scope(|s| {
            //     let jobs: Vec<_> = chunk.chunks(jobs_per_worker).collect();
            //     // debug!("There are {} jobs bundles", jobs.len());
            //
            //     let mut handles = Vec::with_capacity(nb_workers);
            //     for i in 0..nb_workers {
            //         if i < jobs.len() {
            //             let sleep_duration = tx_execution_latency
            //                 .clone().mul(
            //                 2 * jobs.get(i).unwrap().len() as u32
            //             );
            //             handles.push(s.spawn(move |_| {
            //                 sleep(sleep_duration);
            //             }));
            //         }
            //     }
            //
            //     for handle in handles {
            //         match handle.join() {
            //             _ => {
            //                 // sleep(Duration::from())
            //             },
            //         }
            //     }
            //
            //     let exec_duration = exec_start.elapsed();
            //     debug!("({:.2?}) worker pool executed {:?} tx from scheduler {}.",
            //           waited, chunk.len(), scheduler);
            //     debug!("\tTook {:?} using {} workers", exec_duration, jobs.len());
            // });

            start_parallel = Instant::now();
        }

        debug!("Done executing batch");
    };
    //endregion
    let _ = rayon::join(
        || main_thread(schedule_receiver, prefix),
        || tmp.collect::<Vec<_>>(),
    );
    std::mem::drop(tasks);
    let pipeline_latency = pipeline_start.elapsed();
    println!();

    //region Mock sequential execution ===================================================================
    let mut tasks: Vec<usize> = (0..nb_tasks).collect();
    println!("Computing latency of sequential execution:");
    let sequential_start = Instant::now();
    // for _ in tasks {
    //     sleep(task_execution_latency);
    // }
    sleep(tx_execution_latency.mul(nb_tasks as u32));
    //endregion
    let sequential_latency = sequential_start.elapsed();
    debug!("Done sequential execution");
    println!();


    println!("Using these parameters:");
    println!("\tscheduling latency: {:?}", schedule_latency);
    println!("\ttx execution latency: {:?}", tx_execution_latency);
    println!("\tsequential prefix: {:?}", prefix_size);
    println!("\tnb schedulers: {:?}", nb_schedulers);
    println!("\tnb workers: {:?}", nb_workers);
    println!("\tnb chunks per scheduler: {:?}", nb_chunks_per_scheduler);
    println!();
    println!("Mock execution of {} transactions:", nb_tasks);
    println!("\tPipeline takes {:?}", pipeline_latency);
    println!("\tSequential takes {:?}", sequential_latency);
}

fn profile_rayon_latency() {
    let iter = 500;
    let computation_latency = Duration::from_micros(300);
    let mut duration = Duration::from_nanos(0);

    let n = 65536;
    let batch: Vec<_> = (0..n).collect();

    let nb_schedulers = 8;
    let chunk_size = (batch.len()/nb_schedulers) + 1;

    for _ in 0..iter {
        let mut b = batch.clone();
        let start = Instant::now();

        // b.par_drain(..b.len())
        //     .chunks(chunk_size)
        //     .enumerate()
        //     .for_each(|(scheduler_index, chunk)| {
        //         sleep(computation_latency);
        //     });
        let res: Vec<_> =
            b
                .par_drain(..b.len())//.chunks(65536)
                .chunks(chunk_size)
                .enumerate()
                .map(|(scheduler_index, chunk)| {
                    let mut scheduled: Vec<i32> = Vec::with_capacity(chunk.len());
                    let mut postponed: Vec<i32> = Vec::with_capacity(chunk.len());
                    let mut working_set = tinyset::SetU64::with_capacity_and_max(
                        2 * 65536,
                        100 * 65536 as u64
                    );

                    // sleep(computation_latency);

                    'outer: for (index, tx) in chunk.into_iter().enumerate() {
                        for addr in [0, 1] {
                            if working_set.contains(addr as u64) {
                                // Can't add tx to schedule
                                postponed.push(tx);
                                continue 'outer;
                            }
                        }

                        // Can add tx to schedule
                        for addr in [0, 1] {
                            working_set.insert((2 + 2 * index + addr) as u64);
                        }
                        scheduled.push(tx);
                    }

                    (scheduler_index, (scheduled, postponed))
                })
                // .flat_map(|(scheduler_index, chunk)| {
                //     let mut scheduled: Vec<i32> = Vec::with_capacity(chunk.len());
                //     let mut postponed: Vec<i32> = Vec::with_capacity(chunk.len());
                //     let mut working_set = tinyset::SetU64::with_capacity_and_max(
                //         2 * 65536,
                //         100 * 65536 as u64
                //     );
                //     sleep(computation_latency);
                //
                //     scheduled
                // })
                .collect();
        duration = duration.add(start.elapsed())
    }

    let total_latency = duration.div(iter as u32);
    println!("rayon latency = {:?}", total_latency.clone());
    println!("rayon overhead = {:?}", total_latency.sub(computation_latency));
}

fn profile_template() {
    let iter = 500;
    let mut duration = Duration::from_nanos(0);

    for _ in 0..iter {
        let start = Instant::now();

        duration = duration.add(start.elapsed())
    }

    let total_latency = duration.div(iter as u32);
    println!("--- latency = {:?}", total_latency.clone());
}

fn profile_schedule_chunk(mut batch: Vec<Transaction<2, 2>>, iter: usize, chunk_fraction: usize, nb_executors: usize) {
    let mut sequential = SequentialVM::new(100 * batch.len()).unwrap();
    sequential.storage.fill(20 * iter as Word);

    let mut parallel = ConcurrentVM::new(100 * batch.len(), 1, nb_executors).unwrap();
    parallel.storage.content.fill(20 * iter as Word);

    // let mut test = ParallelVM::new(100 * batch.len(), 1, nb_executors).unwrap();
    // test.storage.content.fill(20 * iter as Word);

    let mut schedule_duration = Duration::from_nanos(0);
    let mut sequential_duration = Duration::from_nanos(0);
    let mut parallel_duration = Duration::from_nanos(0);

    let computation = |(scheduler_index, b): (usize, Vec<Transaction<2, 2>>)| {
        let mut scheduled = Vec::with_capacity(b.len());
        let mut postponed = Vec::with_capacity(b.len());
        let mut working_set = ThinSetWrapper::with_capacity_and_max(
            // 2 * b.len(),
            2 * 65536,
            // 65536,
            100 * 65536 as u64
        );

        'outer: for tx in b {
            for addr in tx.addresses.iter() {
                if !working_set.insert(*addr as u64) {
                    // Can't add tx to schedule
                    postponed.push(tx);
                    continue 'outer;
                }
            }

            // Can add tx to schedule
            // for addr in tx.addresses.iter() {
            //     working_set.insert(*addr as u64);
            // }
            scheduled.push(tx);
        }

        (scheduled, postponed)
    };

    batch.truncate(65536/chunk_fraction);

    for _ in 0..iter {
        let mut b = batch.clone();
        // b.truncate(b.len()/8);
        let a = Instant::now();
        computation((0, b));
        // test.schedule_chunk(b);
        let latency = a.elapsed();
        // println!("** one chunk of length {} took {:?}", batch.len(), latency);
        schedule_duration = schedule_duration.add(latency);

        let mut b = batch.clone();
        // b.truncate(b.len()/8);
        let a = Instant::now();
        let _ = sequential.execute(b);
        sequential_duration = sequential_duration.add(a.elapsed());

        let mut b = batch.clone();
        // b.truncate(b.len()/8);
        let a = Instant::now();
        let _ = parallel.execute_round(0, b);
        parallel_duration = parallel_duration.add(a.elapsed());
    }

    println!("For a chunk of {} tx", batch.len());
    println!("\tschedule_chunk latency = {:?}", schedule_duration.div(iter as u32));
    let avg_sequential = sequential_duration.div(iter as u32);
    println!("\tsequential exec latency = {:?} -> {:?}", avg_sequential, avg_sequential.mul(chunk_fraction as u32));
    let avg_parallel = parallel_duration.div(iter as u32);
    println!("\tparallel exec latency = {:?} -> {:?}", avg_parallel, avg_parallel.mul(chunk_fraction as u32));
}

async fn profile_schedule_backlog_single_pass(mut batch: Vec<Transaction<2, 2>>, iter: usize, nb_schedulers: usize) {
    batch.truncate(65536);
    let mut duration = Duration::from_nanos(0);
    let computation_latency = Duration::from_micros(250);

    for _ in 0..iter {
        let mut b = batch.clone();
        let a = Instant::now();
        // let res: Vec<_> = b.clone()
        //     // .chunks(b.len()/nb_schedulers + 1)
        //
        //     // .into_par_iter()
        //     // .chunks(b.len()/nb_schedulers + 1)
        //
        //     // .par_drain(..b.len())//.chunks(65536)
        //     // .chunks(b.len()/nb_schedulers + 1)
        //
        //     .par_chunks(b.len()/nb_schedulers + 1)
        //
        //     .enumerate()
        //     .map(|(scheduler_index, chunk)| {
        //         let a = Instant::now();
        //         let mut scheduled: Vec<ContractTransaction> = Vec::with_capacity(chunk.len());
        //         let mut postponed: Vec<ContractTransaction> = Vec::with_capacity(chunk.len());
        //         // let mut working_set = tinyset::SetU64::with_capacity_and_max(
        //         //     2 * 65536,
        //         //     100 * 65536 as u64
        //         // );
        //         // let mut working_set: ThinSet<StaticAddress, BuildNoHashHasher<StaticAddress>> = ThinSet::with_capacity_and_hasher(2 * 65536, BuildNoHashHasher::default());
        //         // let mut working_set: Box<ThinSet<StaticAddress, BuildNoHashHasher<StaticAddress>>> = Box::new(ThinSet::with_capacity_and_hasher(2 * 65536, BuildNoHashHasher::default()));
        //         let chunk_size = chunk.len();
        //         // 'outer: for tx in chunk {
        //         //     for addr in tx.addresses.iter() {
        //         //         if working_set.contains(addr) {
        //         //             // Can't add tx to schedule
        //         //             postponed.push(tx.clone());
        //         //             continue 'outer;
        //         //         }
        //         //     }
        //         //
        //         //     // Can add tx to schedule
        //         //     scheduled.push(tx.clone());
        //         //     for addr in tx.addresses {
        //         //         working_set.insert(addr);
        //         //     }
        //         // }
        //         sleep(computation_latency);
        //         let latency = a.elapsed();
        //         println!("Scheduler {}: one chunk of length {} took {:?}", scheduler_index, chunk_size, latency);
        //
        //         (scheduler_index, (scheduled, postponed))
        //     }
        //     ).collect();

        // TODO
        // let computation = |chunk| {
        //     let mut scheduled: Vec<ContractTransaction> = Vec::with_capacity(chunk.len());
        //     let mut postponed: Vec<ContractTransaction> = Vec::with_capacity(chunk.len());
        //     let mut working_set = tinyset::SetU64::with_capacity_and_max(
        //         2 * 65536,
        //         100 * 65536 as u64
        //     );
        //
        //     'outer: for tx in chunk {
        //         for addr in tx.addresses.iter() {
        //             if working_set.contains(addr) {
        //                 // Can't add tx to schedule
        //                 postponed.push(tx.clone());
        //                 continue 'outer;
        //             }
        //         }
        //
        //         // Can add tx to schedule
        //         scheduled.push(tx.clone());
        //         for addr in tx.addresses {
        //             working_set.insert(addr);
        //         }
        //     }
        //
        //     (scheduled, postponed)
        // };
        // let inputs: Vec<_> = b.chunks(b.len()/nb_schedulers + 1).collect();
        // let one = tokio::spawn(async move {
        //     sleep(Duration::from_micros(300));
        // });


        // // ???
        // // Good indicator that rayon has some overhead that I don't understand
        // // There must be some false sharing but I don't know where...
        // let res: Vec<_> = b
        //     .chunks(65536/nb_schedulers + 1)
        //     .map(|chunk| {
        //         let mut scheduled: Vec<ContractTransaction> = Vec::with_capacity(chunk.len());
        //         let mut postponed: Vec<ContractTransaction> = Vec::with_capacity(chunk.len());
        //         let mut working_set = tinyset::SetU64::with_capacity_and_max(
        //             2 * 65536,
        //             100 * 65536 as u64
        //         );
        //         (chunk, scheduled, postponed, working_set)
        //     })
        //     .enumerate()
        //     // .par_bridge()   // TODO what is happening?!?!
        //     .map(|(
        //               scheduler_index,
        //               (chunk, mut scheduled, mut postponed, mut working_set)): (usize, (&[ContractTransaction], Vec<ContractTransaction>, Vec<ContractTransaction>, SetU64))| {
        //         let a = Instant::now();
        //
        //         let chunk_size = chunk.len();
        //         'outer: for tx in chunk {
        //             for addr in tx.addresses.iter() {
        //                 if working_set.contains(*addr as u64) {
        //                     // Can't add tx to schedule
        //                     postponed.push(tx.clone());
        //                     continue 'outer;
        //                 }
        //             }
        //
        //             // Can add tx to schedule
        //             for addr in tx.addresses.iter() {
        //                 working_set.insert(*addr as u64);
        //             }
        //             scheduled.push(tx.clone());
        //         }
        //         // sleep(computation_latency);
        //         let latency = a.elapsed();
        //         println!("Scheduler {}: one chunk of length {} took {:?}", scheduler_index, chunk_size, latency);
        //
        //         (scheduler_index, (scheduled, postponed))
        //     }).collect();
        type Set = ThinSetWrapper;      // 390 micro, 650 micro <-- need unsafe impl Send...

        let tmp: Vec<_> = b
            .chunks(65536/nb_schedulers + 1)
            .enumerate()
            .map(|(index, chunk)| {

                let mut scheduled: Box<Vec<Transaction<2, 2>>> = Box::new(Vec::with_capacity(chunk.len()));
                let mut postponed: Box<Vec<Transaction<2, 2>>> = Box::new(Vec::with_capacity(chunk.len()));
                // let mut working_set = Box::new(ThinSet::with_capacity_and_hasher(2 * 65536, BuildNoHashHasher::default()));
                let mut working_set = Box::new(ThinSetWrapper::with_capacity_and_max( // TODO Is faster with ThinSetWrapper
                      2 * 65536,
                      100 * 65536 as u64
                ));
                let mut set = IntSet::with_capacity_and_hasher(2 * 65536, BuildNoHashHasher::default());

                // if index == 0 {
                //     println!("scheduled capacity before: {}", scheduled.capacity());
                //     println!("postponed capacity before: {}", postponed.capacity());
                //     println!("working_set capacity before: {}", working_set.capacity());
                // }
                // (index, chunk, scheduled, postponed, working_set, set)

                let mut chunk_copy = Vec::with_capacity(chunk.len());
                chunk.clone_into(&mut chunk_copy);
                (index, chunk_copy, scheduled, postponed, working_set, set)
            })
            .collect();
        println!("Allocating took {:?}", a.elapsed());
        println!();

        println!("Sequentially:");
        let sequential = Instant::now();
        let res: Vec<(usize, Box<Vec<Transaction<2, 2>>>, Box<Vec<Transaction<2, 2>>>)> = tmp.clone()
            .into_iter()
            .map(|(scheduler_index, chunk, mut scheduled, mut postponed, mut working_set, mut set):
                    // (usize, &[ContractTransaction], Box<Vec<ContractTransaction>>, Box<Vec<ContractTransaction>>, Box<SetU64>, HashSet<u64, BuildNoHashHasher<u64>>)| {
                  (usize, Vec<Transaction<2, 2>>, Box<Vec<Transaction<2, 2>>>, Box<Vec<Transaction<2, 2>>>, Box<ThinSetWrapper>, HashSet<u64, BuildNoHashHasher<u64>>)| {
                let a = Instant::now();

                // let mut fast_path = working_set.clone();

                let chunk_size = chunk.len();
                let mut insertion_latency = Duration::from_micros(0);
                'outer: for tx in chunk {
                    for addr in tx.addresses.iter() {
                        // if set.contains(&(*addr as u64)) {
                        //     // Can't add tx to schedule
                        //     postponed.push(tx.clone());
                        //     continue 'outer;
                        // }
                        if !working_set.insert(*addr as u64) {
                            // Can't add tx to schedule
                            postponed.push(tx.clone());
                            // fast_path = working_set.clone();
                            // for rm_addr in tx.addresses.iter() {
                            //     working_set.remove(*rm_addr as u64);
                            // }
                            continue 'outer;
                        }

                        // fast_path.insert(*addr as u64);
                        // working_set.insert(*addr as u64);
                    }

                    // Can add tx to schedule
                    scheduled.push(tx.clone());
                    // for addr in tx.addresses {
                    //     set.insert(addr as u64);
                    // }
                    // for addr in tx.addresses.iter() {
                    //     working_set.insert(*addr as u64);
                    // }
                    // let insertion_start = Instant::now();
                    // // for addr in tx.addresses {
                    // //     working_set.insert(addr as u64);
                    // // }
                    // working_set = fast_path;
                    // fast_path = tinyset::SetU64::with_capacity_and_max(
                    //     2 * 65536,
                    //     100 * 65536 as u64
                    // );
                    // insertion_latency = insertion_latency.add(insertion_start.elapsed());
                    // println!("Inserting took {:?}", insertion_start.elapsed());
                    // println!();
                }

                // sleep(computation_latency);

                // while a.elapsed() < computation_latency {
                //
                // }
                // if scheduler_index == 0 {
                //     println!("scheduled capacity after: {}", scheduled.capacity());
                //     println!("postponed capacity after: {}", postponed.capacity());
                //     println!("working_set capacity after: {}", working_set.capacity());
                //     // println!("Inserting took {:?} overall", insertion_latency);
                // }
                let latency = a.elapsed();
                debug!("Scheduler {}: one chunk of length {} took {:?}", scheduler_index, chunk_size, latency);

                (scheduler_index, scheduled, postponed)
            }).collect();
        println!("overall latency = {:?}", sequential.elapsed());

        println!();
        println!("Parallel:");
        let parallel = Instant::now();
        let res: Vec<(usize, Box<Vec<Transaction<2, 2>>>, Box<Vec<Transaction<2, 2>>>)> = tmp
            .into_par_iter()
            .map(|(scheduler_index, chunk, mut scheduled, mut postponed, mut working_set, mut set):
                  (usize, Vec<Transaction<2, 2>>, Box<Vec<Transaction<2, 2>>>, Box<Vec<Transaction<2, 2>>>, Box<ThinSetWrapper>, HashSet<u64, BuildNoHashHasher<u64>>)| {
                let a = Instant::now();

                let chunk_size = chunk.len();
                'outer: for tx in chunk {
                    for addr in tx.addresses.iter() {

                        if !working_set.insert(*addr as u64) {
                            // Can't add tx to schedule
                            postponed.push(tx.clone());
                            continue 'outer;
                        }
                    }

                    // Can add tx to schedule
                    scheduled.push(tx.clone());
                }

                let latency = a.elapsed();
                debug!("Scheduler {}: one chunk of length {} took {:?}", scheduler_index, chunk_size, latency);

                (scheduler_index, scheduled, postponed)
            }).collect();
        println!("overall latency = {:?}", parallel.elapsed());

        // let res: Vec<_> = b
        //     .par_drain(..b.len())//.chunks(65536)
        //     .chunks(65536/4)
        //     .enumerate()
        //     .map(|(scheduler_index, chunk)| {
        //         let mut scheduled = [None; 65538/8];
        //         let mut postponed = [None; 65538/8];
        //         let mut scheduled_len = 0;
        //         let mut postponed_len = 0;
        //         let mut working_set = tinyset::SetU64::with_capacity_and_max(
        //             2 * 65536,
        //             100 * 65536 as u64
        //         );
        //
        //         'outer: for tx in chunk {
        //             for addr in tx.addresses.iter() {
        //                 if working_set.contains(*addr as u64) {
        //                     // Can't add tx to schedule
        //                     postponed[postponed_len] = Some(tx);
        //                     postponed_len += 1;
        //                     continue 'outer;
        //                 }
        //             }
        //
        //             // Can add tx to schedule
        //             for addr in tx.addresses.iter() {
        //                 working_set.insert(*addr as u64);
        //             }
        //             scheduled[postponed_len] = Some(tx);
        //             scheduled_len += 1;
        //         }
        //
        //         (scheduler_index, (scheduled, postponed))
        //     }
        //     ).collect();

        // let res: Vec<_> = b
        //     .chunks(65536/8)
        //     .map(|chunk|
        //         (
        //             chunk,
        //             Vec::with_capacity(chunk.len()),
        //             Vec::with_capacity(chunk.len())
        //         )
        //     )
        //     .enumerate()
        //     .map(|(scheduler_index, (
        //         chunk,
        //         mut scheduled,
        //         mut postponed)
        //           )| {
        //         // let mut scheduled = Vec::with_capacity(chunk.len());
        //         // let mut postponed = Vec::with_capacity(chunk.len());
        //         let mut working_set = tinyset::SetU64::with_capacity_and_max(
        //             2 * 65536,
        //             100 * 65536 as u64
        //         );
        //
        //         'outer: for tx in chunk {
        //             for addr in tx.addresses.iter() {
        //                 if working_set.contains(*addr as u64) {
        //                     // Can't add tx to schedule
        //                     postponed.push(tx);
        //                     continue 'outer;
        //                 }
        //             }
        //
        //             // Can add tx to schedule
        //             for addr in tx.addresses.iter() {
        //                 working_set.insert(*addr as u64);
        //             }
        //             scheduled.push(tx);
        //         }
        //
        //         (scheduler_index, (scheduled, postponed))
        //     }
        //     ).collect();
        duration = duration.add(a.elapsed());
    }

    // println!("schedule_backlog_single_pass latency = {:?}", duration.div(iter as u32));
}

async fn test_profile_schedule_backlog_single_pass(mut batch: Vec<Transaction<2, 2>>, iter: usize) {
    batch.truncate(65536);
    let mut duration = Duration::from_nanos(0);
    for _ in 0..iter {
        let mut b = batch.clone();
        let a = Instant::now();

        let nb_schedulers = 8;
        let mut handles: Vec<_> = b.drain(..b.len())
            // .into_iter()
            .chunks(65536/nb_schedulers)
            .into_iter()
            .enumerate()
            .map(|(index, chunk)|{
                let chunk: Vec<_> = chunk.collect();
            tokio::task::spawn(async move {
                let mut scheduled = Vec::with_capacity(65536/nb_schedulers);
                let mut postponed = Vec::with_capacity(65536/nb_schedulers);
                let mut working_set = tinyset::SetU64::with_capacity_and_max(
                    2 * 65536,
                    100 * 65536 as u64
                );

                'outer: for tx in chunk {
                    for addr in tx.addresses.iter() {
                        if working_set.contains(*addr as u64) {
                            // Can't add tx to schedule
                            postponed.push(tx);
                            continue 'outer;
                        }
                    }

                    // Can add tx to schedule
                    for addr in tx.addresses.iter() {
                        working_set.insert(*addr as u64);
                    }
                    scheduled.push(tx);
                }

                (0, (scheduled, postponed))
            })
        }).collect();

        let mut res = Vec::with_capacity(nb_schedulers);
        for handle in handles {
           res.push(handle.await);
        }
        duration = duration.add(a.elapsed());
    }

    println!("schedule_backlog_single_pass latency = {:?}", duration.div(iter as u32));
}

//region set alternatives
#[derive(Copy, Clone, Debug)]
struct MockSet {

}
impl MockSet {
    pub fn with_capacity_and_max(cap: usize, max: u64) -> Self {
        return Self { };
    }
    pub fn contains(&self, el: u64) -> bool {
        false
    }
    pub fn insert(&mut self, el: u64) -> bool {
        true
    }
    #[inline]
    pub fn sort(&mut self) {

    }
}


#[derive(Clone, Debug)]
struct BloomFilterWrapper {
    inner: Bloom<u64>
}
impl BloomFilterWrapper {
    pub fn with_capacity_and_max(cap: usize, max: u64) -> Self {
        let inner = Bloom::new_for_fp_rate(cap, 0.3);
        return Self { inner };
    }
    #[inline]
    pub fn contains(&self, el: u64) -> bool {
        self.inner.check(&el)
    }
    #[inline]
    pub fn insert(&mut self, el: u64) -> bool {
        let prev = self.inner.check(&el);
        self.inner.set(&el);
        prev
    }
    #[inline]
    pub fn sort(&mut self) {

    }
}

#[derive(Clone, Debug)]
struct BTreeSetWrapper {
    inner: BTreeSet<u64>
}
impl BTreeSetWrapper {
    pub fn with_capacity_and_max(cap: usize, max: u64) -> Self {
        let inner = BTreeSet::new();
        return Self { inner };
    }
    #[inline]
    pub fn contains(&self, el: u64) -> bool {
        self.inner.contains(&el)
    }
    #[inline]
    pub fn insert(&mut self, el: u64) -> bool {
        self.inner.insert(el)
    }
    #[inline]
    pub fn sort(&mut self) {

    }
}

#[derive(Clone, Debug)]
struct BTreeMapWrapper {
    pub inner: BTreeMap<u64, bool>
}
impl BTreeMapWrapper {
    pub fn with_capacity_and_max(cap: usize, max: u64) -> Self {
        let inner = BTreeMap::new();
        return Self { inner };
    }
    #[inline]
    pub fn contains(&self, el: u64) -> bool {
        *self.inner.get(&el).unwrap()
    }
    #[inline]
    pub fn insert(&mut self, el: u64) -> bool {
        match self.inner.insert(el, true) {
            None => true,
            Some(status) => !status
        }
    }
    #[inline]
    pub fn sort(&mut self) {

    }
}

#[derive(Copy, Clone, Debug)]
struct SingletonSet {
    el: u64
}
impl SingletonSet {
    pub fn with_capacity_and_max(cap: usize, max: u64) -> Self {
        return Self { el: 0 };
    }
    pub fn contains(&self, el: u64) -> bool {
        el == self.el
    }
    pub fn insert(&mut self, el: u64) -> bool {
        let prev = self.el;
        self.el = el;
        el != prev
    }
    #[inline]
    pub fn sort(&mut self) {

    }
}

#[derive(Clone, Debug)]
struct ThinSetWrapper {
    inner: ThinSet<u64>
}
impl ThinSetWrapper {
    pub fn with_capacity_and_max(cap: usize, max: u64) -> Self {
        let inner = ThinSet::with_capacity(2 * 65536 / 8);
        return Self { inner };
    }
    #[inline]
    pub fn contains(&self, el: u64) -> bool {
        self.inner.contains(&el)
    }
    #[inline]
    pub fn insert(&mut self, el: u64) -> bool {
        self.inner.insert(el)
    }
    #[inline]
    pub fn sort(&mut self) {

    }
}
unsafe impl Send for ThinSetWrapper {}

#[derive(Clone, Debug)]
struct SortedVecWrapper {
    inner: Vec<u64>
}
impl SortedVecWrapper {
    pub fn with_capacity_and_max(cap: usize, max: u64) -> Self {
        let inner = Vec::with_capacity(cap);
        return Self { inner };
    }
    pub fn contains(&self, el: u64) -> bool {
        self.inner.binary_search(&el).is_ok()
    }
    pub fn insert(&mut self, el: u64) -> bool {
        match self.inner.binary_search(&el) {
            Ok(pos) => false, // element already in vector @ `pos`
            Err(pos) => {
                self.inner.insert(pos, el);
                true
            },
        }
    }
    #[inline]
    pub fn sort(&mut self) {
        self.inner.voracious_sort();
    }
}

#[derive(Clone, Debug)]
struct SparseSet {
    inner: Vec<SetU64>,
    remainder: BTreeSet<u64>
}
impl SparseSet {
    pub fn with_capacity_and_max(cap: usize, max: u64) -> Self {
        let nb_subset = max as usize /65536;
        let mut inner = Vec::with_capacity(nb_subset);
        for i in 0..nb_subset {
            inner.push(SetU64::with_capacity_and_max(cap/nb_subset, max/nb_subset as u64));
        }
        let remainder = BTreeSet::new();
        return Self { inner , remainder };
    }
    pub fn contains(&self, el: u64) -> bool {
        for (index, v) in self.inner.iter().enumerate() {
            if (el as usize) < 65536 * (index + 1) {
                let delta = 65536 * index as u64;
                let value = el - delta;
                return v.contains(value);
            }
        }
        return self.remainder.contains(&el);
    }
    pub fn insert(&mut self, el: u64) -> bool {
        for (index, v) in self.inner.iter_mut().enumerate() {
            if (el as usize) < 65536 * (index + 1) {
                let delta = 65536 * index as u64;
                return v.insert(el - delta);
            }
        }
        return self.remainder.insert(el);
    }
    #[inline]
    pub fn sort(&mut self) {

    }
}

// #[derive(Copy, Clone, Debug)]
// struct ExpensiveSet {
//     inner: Vec<bool>
// }
// impl ExpensiveSet {
//     pub fn with_capacity_and_max(cap: usize, max: u64) -> Self {
//         let inner = Vec::with_capacity(max as usize);
//         return Self { inner };
//     }
//     pub fn contains(&self, el: u64) -> bool {
//         self.inner[el as usize]
//     }
//     pub fn insert(&mut self, el: u64) -> bool {
//         let prev = self.inner[el as usize];
//         self.inner[el as usize] = true;
//         prev
//     }
// }
//endregion

async fn try_other_method(mut batch: Vec<Transaction<2, 2>>, iter: usize, nb_schedulers: usize, parallel_schedule: bool, nb_executors: usize,) {

    let mut duration = Duration::from_nanos(0);
    // let nb_schedulers = 8;
    // let nb_executors = 8;
    let chunk_size = batch.len()/nb_schedulers + 1;

    let parallel_init = true;
    // let parallel_schedule = false;

    // // type Set = <ALTERNATIVE>;  // <SEQUENTIAL TIME>, <PARALLEL TIME>
    // type Set = MockSet;             // 60 micro, 90 micro
    // type Set = SingletonSet;        // 70 micro, 120 micro
    // type Set = SetU64;              // 550 micro, 1.9-2.7 ms
    // type Set = BTreeSetWrapper;     // 2.4 ms, 3.5-4.7 ms
    type Set = ThinSetWrapper;      // 390 micro, 650 micro <-- need unsafe impl Send...
    // type Set = SparseSet;           // 1.5 ms, 2.5 ms
    // type Set = SortedVecWrapper;    // 35 ms, 70 ms
    // type Set = BTreeMapWrapper;     // 2.25 ms, 2.7-3.5 ms
    // type Set = BloomFilterWrapper;  // 1.4 ms, 2.4 ms

    for _ in 0..iter {
        let mut b = batch.clone();
        let init_start = Instant::now();

        let mut output = Arc::new(Mutex::new(Vec::with_capacity(b.len())));

        // let mut intermediate: Vec<Box<(Vec<ContractTransaction>, Vec<ContractTransaction>, Vec<ContractTransaction>, SetU64)>> =
        let mut intermediate: Vec<_> =
            if parallel_init {
                b
                    .into_par_iter()
                    .chunks(chunk_size)
                    .map(|chunk| {
                        let scheduled = Vec::with_capacity(chunk.len());
                        let postponed = Vec::with_capacity(chunk.len());
                        let mut addresses = Set::with_capacity_and_max(
                            2 * 65536,
                            100 * 65536 as u64
                        );

                        // for tx in chunk.iter() {
                        //     for addr in tx.addresses.iter() {
                        //         addresses.inner.insert(*addr as u64, false);
                        //     }
                        // }
                        Box::new((chunk, scheduled, postponed, addresses))
                    })
                    .collect()
            } else
            {
                b
                    .into_iter()
                    .chunks(chunk_size)
                    .into_iter()
                    .map(|chunk| {
                        let chunk: Vec<_> = chunk.collect();
                        let scheduled = Vec::with_capacity(chunk.len());
                        let postponed = Vec::with_capacity(chunk.len());
                        let addresses = Set::with_capacity_and_max(
                            2 * 65536,
                            100 * 65536 as u64
                        );

                        Box::new((chunk, scheduled, postponed, addresses))
                    })
                    .collect()
            };

        let mut storage = VmStorage::new(100 * 65536);
        storage.content.fill(200);
        println!("Preparing intermediate data took {:?}\n", init_start.elapsed());

        let schedule_exec_start = Instant::now();
        let mut synchro: Option<JoinHandle<()>> = None;
        for (index, mut boxed) in intermediate.into_iter().enumerate() {

            let previous = synchro;
            let acc = output.clone();
            let mut shared_storage = storage.get_shared();
            let functions: Vec<_> = AtomicFunction::all();

            let handle = tokio::spawn(async move {
                let (
                    chunk,
                    ref mut scheduled,
                    ref mut postponed,
                    ref mut addresses) = boxed.as_mut();
                let schedule_start = Instant::now();

                // Schedule transactions
                'outer: for (index, tx) in chunk.iter().enumerate() {
                    for addr in tx.addresses.iter() {
                        if !addresses.insert(*addr as u64) {
                            // Can't add tx to schedule
                            postponed.push(tx.clone());
                            continue 'outer;
                        }
                        // addresses.insert(*addr as u64);
                        // tmp_addresses.insert(*addr as u64);
                    }

                    // Can add tx to schedule
                    // for addr in tx.addresses.iter() {
                    //     addresses.insert(*addr as u64);
                    // }
                    // addresses.sort();
                    scheduled.push(tx.clone());
                }
                let scheduling_latency = schedule_start.elapsed();
                debug!("Task {} took {:?} to schedule", index, scheduling_latency);

                match previous {
                    None => {
                        // can start execution immediately
                    },
                    Some(handle) => {
                        // can start execution once the previous scheduler is done
                        let _ = handle.await;
                    }
                }

                // println!("Task {} took {:?} to schedule", index, scheduling_latency);
                // Execute transactions
                let exec_start = Instant::now();
                let mut tmp: Vec<_> = scheduled
                    // .into_par_iter()
                    // .par_drain(..round.len())
                    // .chunks(chunk_size)
                    .par_chunks(scheduled.len()/ nb_executors + 1)
                    .enumerate()
                    .map(
                        |(worker_index, worker_backlog)| {
                            // println!("Worker {} works on {} tx", worker_index, worker_backlog.len());
                            worker_backlog
                                // .drain(..worker_backlog.len())
                                .into_iter()
                                .flat_map(|tx| {
                                    // let function = functions.get(tx.function as usize).unwrap();
                                    let function = tx.function;
                                    match unsafe { function.execute(tx.clone(), shared_storage) } {
                                        Another(generated_tx) => Some(generated_tx),
                                        _ => None,
                                    }
                                })
                                .collect::<Vec<Transaction<2, 2>>>()
                        }
                    ).collect();
                let mut generated_tx = acc.lock().unwrap();
                for mut generated in tmp {
                    generated_tx.append(&mut generated);
                }
                generated_tx.append(postponed);
                // generated_tx.append(&mut generated);
                debug!("Task {} took {:?} to execute ---------------", index, exec_start.elapsed());
            });

            if parallel_schedule {
                synchro = Some(handle);
            } else {
                let _ = handle.await;
                synchro = None;
            }
        }

        match synchro {
            Some(handle) => {
                // Wait last scheduler to finish executing
                let _ = handle.await;
                // execution is complete

            },
            None if parallel_schedule => {
                panic!("Last handle is missing");
            },
            _ => {

            }
        }

        let elapsed = schedule_exec_start.elapsed();
        // println!("Scheduling-execution loop done in {:?}", schedule_exec_start.elapsed());
        // let mut generated_tx = output.lock().unwrap();
        // println!("{} txs have been generated or postponed", generated_tx.len());
        duration = duration.add(elapsed);
    }

    println!("other method latency = {:?}", duration.div(iter as u32));
}

fn try_scheduling_sequential(batch: &Vec<T>, storage_size: usize) {
    println!("Scheduled addresses using IntSet...");
    let mut scheduled_addresses: IntSet<u64> = IntSet::default();
    let a = Instant::now();
    for tx in batch.iter() {
        if !scheduled_addresses.contains(&tx.from) &&
            !scheduled_addresses.contains(&tx.to) {

            scheduled_addresses.insert(tx.from);
            scheduled_addresses.insert(tx.to);
        }
    }
    let elapsed = a.elapsed();
    println!("nb scheduled addresses: {}", scheduled_addresses.len());
    println!("Took {:?}", elapsed);
    println!();
    println!("-------------------------");

    println!("Scheduled addresses using tiny set...");
    let mut scheduled_addresses = tinyset::SetU64::with_capacity_and_max(2*batch.len(), storage_size as u64);
    let a = Instant::now();
    for tx in batch.iter() {
        if !scheduled_addresses.contains(tx.from) &&
            !scheduled_addresses.contains(tx.to) {

            scheduled_addresses.insert(tx.from);
            scheduled_addresses.insert(tx.to);
        }
    }
    let elapsed = a.elapsed();
    println!("nb scheduled addresses: {}", scheduled_addresses.len());
    println!("Took {:?}", elapsed);
    println!();
    println!("-------------------------");

    println!("Scheduled addresses using bloom filter...");
    let mut nb_scheduled_addresses = 0;
    let mut scheduled_addresses = Bloom::new_for_fp_rate(2*batch.len(), 0.1);
    let a = Instant::now();
    for tx in batch.iter() {
        let from_is_scheduled = scheduled_addresses.check(&tx.from);
        let to_is_scheduled = scheduled_addresses.check(&tx.to);
        if !from_is_scheduled && !to_is_scheduled {
            scheduled_addresses.set(&tx.from);
            scheduled_addresses.set(&tx.to);
            nb_scheduled_addresses += (1 - from_is_scheduled as u64) + (1 - to_is_scheduled as u64);
        }
    }
    let elapsed = a.elapsed();
    println!("nb scheduled addresses: {}, {:?}", nb_scheduled_addresses, scheduled_addresses.clear());
    println!("Took {:?}", elapsed);
    println!();
}

fn try_scheduling_parallel(batch: &Vec<T>, storage_size: usize) {
    println!("Sequential schedule...");
    let set_capacity = 2 * batch.len();
    let mut scheduled_addresses = tinyset::SetU64::with_capacity_and_max(set_capacity, storage_size as u64);

    let a = Instant::now();
    let mut delayed_tx = vec!();
    for tx in batch.iter() {
        if !scheduled_addresses.contains(tx.from) &&
            !scheduled_addresses.contains(tx.to) {

            scheduled_addresses.insert(tx.from);
            scheduled_addresses.insert(tx.to);
        } else {
            delayed_tx.push(tx.clone());
        }
    }
    let elapsed = a.elapsed();
    println!("nb scheduled addresses: {}", scheduled_addresses.len());
    println!("nb delayed tx: {}", delayed_tx.len());
    println!("Took {:?}", elapsed);
    println!();
    println!("-------------------------");

    println!("Parallel schedule (2 tasks)...");
    {
        let set_capacity = batch.len();

        let a = Instant::now();

        let mut fist_scheduled_addresses = tinyset::SetU64::with_capacity_and_max(set_capacity, storage_size as u64);
        let mut second_scheduled_addresses = tinyset::SetU64::with_capacity_and_max(set_capacity, storage_size as u64);
        let first_half = &batch[0..batch.len()/2];
        let second_half = &batch[batch.len()/2..];
        let mut parallel = vec![
            (first_half.into_iter(), &mut fist_scheduled_addresses),
            (second_half.into_iter(), &mut second_scheduled_addresses)];
        // println!("**Creating chunks takes {:?}", a.elapsed());

        let b = Instant::now();
        let result: Vec<(Vec<usize>, Vec<T>)> = parallel
            .par_iter_mut()
            .map(|(chunk, schedule)| {
                let mut delayed_tx = vec!();
                let mut scheduled_tx = Vec::with_capacity(chunk.len());
                for (tx_index, tx) in chunk.enumerate() {
                    if !schedule.contains(tx.from) &&
                        !schedule.contains(tx.to) {

                        schedule.insert(tx.from);
                        schedule.insert(tx.to);
                        scheduled_tx.push(tx_index);
                    } else {
                        delayed_tx.push(tx.clone());
                    }
                }

                (scheduled_tx, delayed_tx)
            }
        ).collect();
        let elapsed = a.elapsed();
        // println!("**creating schedule took {:?}", b.elapsed());

        let execute_now = result[0].0.len() + result[1].0.len();
        let execute_later = result[0].1.len() + result[1].1.len();

        // println!("Total to execute (excl conflicts): {}", execute_now);
        // println!("Total to delay (excl conflicts): {}", execute_later);
        // println!("Total: {}", execute_now + execute_later);
        println!("Took {:?}", elapsed);
        println!();

        println!("Checking for conflicts between chunks...");
        let a = Instant::now();
        let mut conflicts = 0;
        for tx_index in result[0].0.iter() {
            let tx = batch[*tx_index];
            if second_scheduled_addresses.contains(tx.from) || second_scheduled_addresses.contains(tx.to) {
                conflicts += 1;
            }
        }
        let elapsed = a.elapsed();
        println!("nb conflicts among halves: {}", conflicts);
        println!("Took {:?}", elapsed);
        //
        // println!();
        // let to_execute_now = result[0].0.len() + result[1].0.len() - conflicts;
        // let to_execute_later = result[0].1.len() + result[1].1.len() + conflicts;
        // let total_over_two_rounds = to_execute_now + to_execute_later;
        // println!("Total number of tx to execute this round: {}", to_execute_now);
        // println!("Total number of tx to execute next round: {}", to_execute_later);
        // println!("Total number over both rounds: {}", total_over_two_rounds);
    }


    println!("-------------------------");

    println!("Parallel schedule (K tasks) v1...");
    {
        let nb_tasks = 2;
        println!("K = {}", nb_tasks);
        let chunk_size = batch.len()/nb_tasks;
        let set_capacity = 2 * batch.len() / nb_tasks;

        let a = Instant::now();

        let mut chunks: Vec<_> = batch
            .par_chunks(chunk_size)
            .enumerate()
            .map(|(chunk_index, tx_chunk)| {
                let mut scheduled_addr = tinyset::SetU64::with_capacity_and_max(set_capacity, storage_size as u64);
                let mut scheduled_tx = Vec::with_capacity(tx_chunk.len());
                let mut delayed_tx = vec!();

                for (index_in_chunk, tx) in tx_chunk.iter().enumerate() {
                    let tx_index  = chunk_index * chunk_size + index_in_chunk;
                    if !scheduled_addr.contains(tx.from) &&
                        !scheduled_addr.contains(tx.to) {

                        scheduled_addr.insert(tx.from);
                        scheduled_addr.insert(tx.to);
                        scheduled_tx.push(tx_index);
                    } else {
                        delayed_tx.push(tx_index);
                    }
                }

                (scheduled_addr, scheduled_tx, delayed_tx)
            }
            ).collect();
        let elapsed = a.elapsed();

        // let mut execute_now = Vec::with_capacity(batch.len());
        // let mut execute_later = vec!();
        // let actual_scheduled_addr = tinyset::SetU64::with_capacity_and_max(set_capacity, storage_size as u64);
        let mut execute_now = 0;
        let mut execute_later = 0;
        for (chunk_index, chunk) in chunks.iter().enumerate() {
            // println!("Chunk {}:", chunk_index);
            // println!("\tscheduled addresses: {}", chunk.0.len());
            // println!("\tscheduled tx: {}", chunk.1.len());
            // println!("\tdelayed tx: {}", chunk.2.len());

            execute_now += chunk.1.len();
            execute_later += chunk.2.len();
        }
        // println!("Total to execute (excl conflicts): {}", execute_now);
        // println!("Total to delay (excl conflicts): {}", execute_later);
        // println!("Total: {}", execute_now + execute_later);
        println!("Took {:?}", elapsed);
    }

    println!("-------------------------");

    println!("Parallel schedule (K tasks) v2...");
    {
        let nb_tasks = 2;
        println!("K = {}", nb_tasks);
        let chunk_size = batch.len()/nb_tasks;
        let set_capacity = 2 * batch.len() / nb_tasks;

        let a = Instant::now();

        let mut chunks: Vec<_> = batch.par_chunks(chunk_size).enumerate().map(|(chunk_index, tx_chunk)| {
            let scheduled_addr = tinyset::SetU64::with_capacity_and_max(set_capacity, storage_size as u64);
            (tx_chunk, scheduled_addr, chunk_index)
        }).collect();
        // println!("**Creating chunks takes {:?}", a.elapsed());

        let b = Instant::now();
        let result: Vec<_> = chunks
            .par_iter_mut()
            .map(|(chunk, scheduled_addr, chunk_index)| {
                let mut delayed_tx = vec!();
                let mut scheduled_tx = Vec::with_capacity(chunk.len());
                for (index_in_chunk, tx) in chunk.iter().enumerate() {
                    // let tx_index  = *chunk_index * chunk_size + index_in_chunk;
                    let tx_index = index_in_chunk;
                    if !scheduled_addr.contains(tx.from) &&
                        !scheduled_addr.contains(tx.to) {

                        scheduled_addr.insert(tx.from);
                        scheduled_addr.insert(tx.to);
                        scheduled_tx.push(tx_index);
                    } else {
                        delayed_tx.push(tx_index);
                    }
                }

                (scheduled_tx, delayed_tx)
            }
            ).collect();
        let elapsed = a.elapsed();
        // println!("**creating schedule took {:?}", b.elapsed());

        let mut execute_now = 0;
        let mut execute_later = 0;

        for res in result.iter() {
            execute_now += res.0.len();
            execute_later += res.1.len();
        }
        // println!("Total to execute (excl conflicts): {}", execute_now);
        // println!("Total to delay (excl conflicts): {}", execute_later);
        // println!("Total: {}", execute_now + execute_later);
        println!("Took {:?}", elapsed);
        println!();
    }

    // println!("Parallel schedule (merge sort)...");
    {
        // let nb_workers = 4;
        // let start = Instant::now();
        // let rounds = merge_sort(batch.as_slice(), storage_size, 0, 0);
        // let merge_sort_latency = start.elapsed();
        // println!("There are {} rounds:", rounds.len());
        // let mut execution_latency = Duration::from_secs(0);
        // let mut generated_tx = vec!();
        // for (round_index, (addr, round)) in rounds.iter().enumerate() {
        //     println!("Executing round {} ({} tx)", round_index, round.len());
        //     let start = Instant::now();
        //     let mut tmp: Vec<_> = round
        //         .par_chunks(round.len()/4 + 1)
        //         .enumerate()
        //         .flat_map(|(worker_index, worker_backlog)| {
        //             let worker_output: Vec<_> = worker_backlog
        //                 // .drain(..worker_backlog.len())
        //                 .into_iter()
        //                 .flat_map(|tx| {
        //                     // execute the transaction and optionally generate a new tx
        //                     let function = self.functions.get(tx.function as usize).unwrap();
        //                     match unsafe { function.execute(tx.clone(), self.storage.get_shared()) } {
        //                         Another(generated_tx) => Some(generated_tx),
        //                         _ => None,
        //                     }
        //                 })
        //                 .collect();
        //             worker_output
        //         }).collect();
        //     generated_tx.append(&mut tmp);
        //     execution_latency = execution_latency.add(start.elapsed());
        // }
    //     let set_capacity = 2 * batch.len();
    //
    //     let a = Instant::now();
    //
    //     let mut chunks: Vec<_> = batch
    //         .par_chunks(2)
    //         .enumerate()
    //         .map(|(chunk_index, tx_chunk)| {
    //             let mut scheduled_addr = tinyset::SetU64::with_capacity_and_max(set_capacity, storage_size as u64);
    //             let mut scheduled_tx = Vec::with_capacity(tx_chunk.len());
    //             let mut delayed_tx = vec!();
    //
    //             for (index_in_chunk, tx) in tx_chunk.iter().enumerate() {
    //                 if !scheduled_addr.contains(tx.from) &&
    //                     !scheduled_addr.contains(tx.to) {
    //
    //                     scheduled_addr.insert(tx.from);
    //                     scheduled_addr.insert(tx.to);
    //                     scheduled_tx.push(tx_index);
    //                 } else {
    //                     delayed_tx.push(tx_index);
    //                 }
    //             }
    //
    //             (scheduled_addr, scheduled_tx, delayed_tx)
    //         }
    //         ).collect();
    //     let elapsed = a.elapsed();
    //
    //     // let mut execute_now = Vec::with_capacity(batch.len());
    //     // let mut execute_later = vec!();
    //     // let actual_scheduled_addr = tinyset::SetU64::with_capacity_and_max(set_capacity, storage_size as u64);
    //     let mut execute_now = 0;
    //     let mut execute_later = 0;
    //     for (chunk_index, chunk) in chunks.iter().enumerate() {
    //         // println!("Chunk {}:", chunk_index);
    //         // println!("\tscheduled addresses: {}", chunk.0.len());
    //         // println!("\tscheduled tx: {}", chunk.1.len());
    //         // println!("\tdelayed tx: {}", chunk.2.len());
    //
    //         execute_now += chunk.1.len();
    //         execute_later += chunk.2.len();
    //     }
    //     // println!("Total to execute (excl conflicts): {}", execute_now);
    //     // println!("Total to delay (excl conflicts): {}", execute_later);
    //     // println!("Total: {}", execute_now + execute_later);
    //     println!("Took {:?}", elapsed);
    }

    println!("-------------------------");
}

async fn try_merge_sort(batch: &Vec<T>, storage_size: usize) {
    let depth_limit = 3;
    println!("Merge with depth {}", depth_limit);
    let start = Instant::now();
    let res = merge_sort(batch.as_slice(), storage_size, 0, depth_limit);
    let duration = start.elapsed();
    println!("Need {} rounds", res.len());
    println!("Took {:?}", duration);
    // for (round, (_, scheduled_tx)) in res.iter().enumerate() {
    //     println!("\tRound {} has {} txs", round, scheduled_tx.len());
    // }

//     let _ = tokio::spawn(async move {
//         let start = Instant::now();
//         let res = merge_sort_async(batch.as_slice(), storage_size, 0, 1).await.unwrap();
//         // recursive(root_path, tx, ext).await.unwrap();
//         let duration = start.elapsed();
//         println!("Need {} rounds", res.len());
//         println!("Took {:?}", duration);
//     }).await;
}

fn merge_sort(chunk: &[T], storage_size: usize, depth: usize, depth_limit: usize) -> Vec<(ThinSetWrapper, Vec<&T>)> {

    return if depth >= depth_limit {
        let start = Instant::now();
        let set_capacity = chunk.len() * 2;
        // let set_capacity = 65536 * 2;
        let mut rounds = vec![(
            ThinSetWrapper::with_capacity_and_max(set_capacity, storage_size as u64),
            vec!()
        )];
        'outer: for tx in chunk.iter() {
            for (round_addr, round_tx) in rounds.iter_mut() {
                if !round_addr.contains(tx.from) &&
                    !round_addr.contains(tx.to) {
                    round_addr.insert(tx.from);
                    round_addr.insert(tx.to);
                    round_tx.push(tx);
                    continue 'outer;
                }
            }
            // Can't be added to any round
            let new_round_addr = ThinSetWrapper::with_capacity_and_max(set_capacity, storage_size as u64);
            let new_round_tx = vec!(tx);
            rounds.push((new_round_addr, new_round_tx));
        }

        let elapsed = start.elapsed();
        println!("\tBase of recursion took {:?}; Chunk has size {}", elapsed, chunk.len());
        rounds
    } else {
        let middle = chunk.len() / 2;

        let (lo, hi) = chunk.split_at(middle);

        let (mut left, mut right) = rayon::join(
            || merge_sort(lo, storage_size, depth + 1, depth_limit),
            || merge_sort(hi, storage_size, depth + 1, depth_limit)
        );

        let mut left_round_index = 0;

        left.append(&mut right);

        return left;

        let mut additional_rounds = vec!();
        'right: for (mut right_addr, mut right_tx) in right.into_iter() {
            'left: for round in left_round_index..left.len() {
                let left_addr = &mut left[round].0;

                'addr: for addr in right_addr.inner.iter() {
                    if left_addr.contains(*addr) {
                        // An address overlap, can't be added to this round

                        // // Try the next rounds
                        // continue 'left;

                        /* TODO Consider which accesses the least addresses to decide what to do?
                            Because if the
                        */
                        // // Don't bother trying the next rounds
                        additional_rounds.push((right_addr.clone(), right_tx));
                        continue 'right;
                    }

                    // TODO
                    // try to simply append the rounds when merging
                    // and use linked list for the schedules
                    //
                    // Graph coloring give optimal scheduling but we is NP-complete
                    //
                    // We don't need the optimal solution, we simply need a schedule that is fast enough
                    // to offset the overhead of planning and synchronisation
                    // -> fast graph coloring approximation
                    //
                    // We don't actually need the whole solution before we start executing.
                    // In particular, new tx are added regularly
                    // -> pipeline is better suited
                    // -> stream processing graph coloring (approximation)
                    //
                    // c.f. symmetry breaking
                    //
                    // graph coloring without explicit edges?
                    //
                    // https://arxiv.org/pdf/2002.10142.pdf
                    //
                    // Keep:
                    //     - serial version
                    //     - single threaded whole batch scheduling
                    //     - single threaded pipeline
                    //     - multi threaded pipeline (split batch in chunks and each thread try to schedule a chunk)
                }

                // No addresses overlap, can be added to this round
                // 'addr: for addr in right_addr.iter() {
                //     left_addr.insert(addr);
                // }
                let left_tx = &mut left[round].1;
                left_tx.append(&mut right_tx);

                // Try to merge the rounds after this one
                left_round_index = round + 1;
                continue 'right;
            }

            // Can't be added to any round
            let new_round_addr = right_addr;
            let new_round_tx = right_tx;
            left.push((new_round_addr, new_round_tx));
            // Don't try to merge the next rounds, just append them
            left_round_index = left.len();
        }

        // println!("Additional rounds: {}", additional_rounds.len());
        left.append(&mut additional_rounds);

        left
    }
}

fn try_sorting(batch: &Vec<T>) {
    let mut to_sort: Vec<_> = batch.iter().map(|tx| tx.from).collect();
    let a = Instant::now();
    to_sort.sort();
    let elapsed = a.elapsed();
    let t: Vec<_> = to_sort.iter().take(10).collect();
    println!("sorted: {:?}", t);
    println!("Took {:?}", elapsed);
    println!("---------------------------------------------------");

    let mut to_sort: Vec<_> = batch.iter().map(|tx| tx.from).collect();
    let a = Instant::now();
    to_sort.voracious_sort();
    let elapsed = a.elapsed();
    let t: Vec<_> = to_sort.iter().take(10).collect();
    println!("voracious sorted: {:?}", t);
    println!("Took {:?}", elapsed);
    println!("---------------------------------------------------");

    let mut to_sort: Vec<_> = batch.iter().map(|tx| tx.from).collect();
    let a = Instant::now();
    to_sort.voracious_mt_sort(4);
    let elapsed = a.elapsed();
    let t: Vec<_> = to_sort.iter().take(10).collect();
    println!("voracious sorted: {:?}", t);
    println!("Took {:?}", elapsed);

    println!();
}

fn try_int_set(batch: &Vec<T>, rng: &mut StdRng, storage_size: usize) {

    println!("IntSet: ------------------------------");
    println!("Computing access sets... ");
    let mut sets = vec![IntSet::default(); batch.len()];
    let a = Instant::now();
    let accesses: Vec<_> = sets.par_iter_mut().enumerate().map(|(i, set)| {
        let tx = batch.get(i).unwrap();
        set.insert(tx.from);
        set.insert(tx.to);
        (*tx, set)
    }).collect();
    let elapsed = a.elapsed();
    println!("accesses len: {:?}", accesses.len());
    println!("Took {:?}", elapsed);
    println!();

    println!("Making set containing all addresses... ");
    let mut address_set: IntSet<u64> = IntSet::default();
    let a = Instant::now();
    for tx in batch.iter() {
        address_set.insert(tx.from);
        address_set.insert(tx.to);
    }
    let addresses: Vec<u64> = address_set.into_iter().collect();
    let elapsed = a.elapsed();
    println!("Nb diff addresses: {:?}", addresses.len());
    println!("Took {:?}", elapsed);
    println!();

    // println!("Checking for conflicts... ");
    // let a = Instant::now();
    // // TODO use a fixed size array instead of a vec?
    // type B = (usize, (T, Set));
    // let (conflicting, parallel): (Vec<B>, Vec<B>) = accesses
    //     .into_iter()
    //     .enumerate()
    //     // .map(|(index, (tx, set))| { (tx, index, set)})
    //     // .zip(indices.into_par_iter())
    //     .partition(|(tx_index, (tx, set))| {
    //         let mut conflicts = false;
    //         for (other_index, other_tx) in batch.iter().enumerate() {
    //             if (*tx_index != other_index) &&
    //                 (set.contains(&other_tx.from) || set.contains(&other_tx.to)) {
    //                 // println!("conflict between: \n{:?} and \n{:?}", tx, other_tx);
    //                 // panic!("!!!");
    //                 conflicts = true;
    //                 break;
    //             }
    //         }
    //         conflicts
    //     });
    //
    // println!("Took {:?}", a.elapsed());
    // println!();
    //
    // println!("Conflicts: {:?}", conflicting.len());
    // println!("Non-conflicting: {:?}", parallel.len());

    // println!("Creating conflict matrix... ");
    // let a = Instant::now();
    // let test: Vec<(T, usize, Vec<bool>)> = accesses
    //     .into_par_iter()
    //     .enumerate()
    //     .map(|(index, (tx, set))| {
    //         let mut conflicts = vec![false; batch_size];
    //         for further_index in (index+1)..batch_size {
    //             let other_tx = batch.get(further_index).unwrap();
    //             let does_conflict = set.contains(&other_tx.from) || set.contains(&other_tx.to);
    //             conflicts[further_index] = does_conflict;
    //         }
    //         (tx, index, conflicts)
    //     }).collect();
    // println!("Took {:?}", a.elapsed());
    // println!("test len: {:?}", test.len());
    // println!();
}

fn try_tiny_set(batch: &Vec<T>, rng: &mut StdRng, storage_size: usize) {

    println!("tiny set: ------------------------------");
    println!("Computing access sets... ");
    let mut sets = vec![
        tinyset::SetU64::with_capacity_and_max(2, storage_size as u64);
        batch.len()
    ];
    let a = Instant::now();
    let accesses: Vec<_> = sets.par_iter_mut().enumerate().map(|(i, set)| {
        let tx = batch.get(i).unwrap();
        set.insert(tx.from);
        set.insert(tx.to);
        (*tx, set)
    }).collect();
    let elapsed = a.elapsed();
    println!("accesses len: {:?}", accesses.len());
    println!("Took {:?}", elapsed);
    println!();

    println!("Making set containing all addresses... ");
    let a = Instant::now();
    let mut b = tinyset::SetU64::with_capacity_and_max(2*batch.len(), storage_size as u64);
    for tx in batch.iter() {
        b.insert(tx.from);
        b.insert(tx.to);
    }

    let addresses: Vec<u64> = b.into_iter().collect();
    let elapsed = a.elapsed();
    println!("Nb diff addresses: {:?}", addresses.len());
    println!("Took {:?}", elapsed);
    println!();

    // println!("Filling conflict matrix...");
    // let mut addr_to_tx: Vec<Vec<bool>> = vec![
    //     vec![false; addresses.len()]; batch.len()
    // ];
    // let a = Instant::now();
    //
    // addr_to_tx.par_iter_mut().enumerate().for_each(|(tx_index, tx_column)| {
    //     for addr_index in 0..addresses.len() {
    //         let addr = addresses.get(addr_index).unwrap();
    //         let pair = accesses.get(tx_index).unwrap();
    //         tx_column[addr_index] = pair.1.contains(*addr);
    //     }
    // });
    //
    // let elapsed = a.elapsed();
    // println!("empty matrix has {:?} columns", addr_to_tx.len());
    // println!("Took {:?}", elapsed);
    // println!();
}

fn try_sequential(batch: &Vec<T>, rng: &mut StdRng, storage_size: usize) {
    //region Sequential ----------------------------------------------------------------------------
    println!("Generating access sets and computing conflict edges...");
    let n = batch.len();
    let tx_to_edge = |i, j| -> usize {
        i + 1 + ((2 * n - j - 1) * j)/2   // indexed from 0 to n-1

        //https://www.ibm.com/docs/en/essl/6.1?topic=representation-lower-packed-storage-mode
        // i + ((2 * n - j) * (j - 1))/2   // indexed from 1 to n
    };
    let nb_edges = (n*(n+1))/2;
    let max_index = tx_to_edge(n-1, n-1);

    let mut edges = vec![false; nb_edges];
    let mut sets_mut = vec![
        tinyset::SetU64::with_capacity_and_max(2, storage_size as u64);
        n
    ];
    let mut sets_ref = vec![
        tinyset::SetU64::with_capacity_and_max(2, storage_size as u64);
        n
    ];


    let a = Instant::now();
    for (i, i_tx) in batch.iter().enumerate() {
        let mut i_set = sets_mut.get_mut(i).unwrap();
        i_set.insert(i_tx.from);
        i_set.insert(i_tx.to);
        sets_ref[i] = i_set.clone();

        for j in 0..i {
            let e = tx_to_edge(i, j);
            let j_set = sets_ref.get(j).unwrap();
            for addr in j_set.iter() {
                // if i_set.contains(addr) {
                //     edges[e] = true;
                //     break;
                // }
                edges[e] = edges[e] || i_set.contains(addr);
            }
        }
    }

    // let addresses: Vec<u64> = address_set.into_iter().collect();
    let elapsed = a.elapsed();
    println!("Size of matrix: {:?}", edges.len());
    println!("Took {:?}", elapsed);
    println!();
    //endregion
}

fn try_parallel_mutex(batch: &Vec<T>, rng: &mut StdRng, storage_size: usize) -> Duration {
    //region parallel mutex ------------------------------------------------------------------------
    println!("Parallel (mutex) generating access sets and computing conflict edges...");
    let n = batch.len();
    let mut sets = vec![
        tinyset::SetU64::with_capacity_and_max(2, storage_size as u64);
        n
    ];

    let a = Instant::now();
    // for (i, i_tx) in batch.iter().enumerate() {
    //     let mut i_set = sets.get_mut(i).unwrap();
    //     i_set.insert(i_tx.from);
    //     i_set.insert(i_tx.to);
    // }
    // for (i, i_set) in sets.iter_mut().enumerate() {
    //     let mut i_tx = batch.get(i).unwrap();
    //     i_set.insert(i_tx.from);
    //     i_set.insert(i_tx.to);
    // }
    sets.par_iter_mut().enumerate().for_each(|(i, set)|{
        let tx = batch.get(i).unwrap();
        set.insert(tx.from);
        set.insert(tx.to);
    });
    // let mut sets: Vec<_> = batch.par_iter().map(|tx| {
    //     let mut set = tinyset::SetU64::with_capacity_and_max(2, storage_size as u64);
    //     set.insert(tx.from);
    //     set.insert(tx.to);
    //
    //     set
    // }).collect();
    // let accesses: Vec<_> = sets.par_iter_mut().enumerate().map(|(i, set)|{
    //     let tx = batch.get(i).unwrap();
    //     set.insert(tx.from);
    //     set.insert(tx.to);
    //     set
    // }).collect();
    let elapsed = a.elapsed();
    println!("Creating sets took {:?}", elapsed);

    // println!();
    // println!("Filling conflict matrix...");
    // let mut edges = Vec::with_capacity(n*n);
    // for _ in 0..(n*n) {
    //     edges.push(Mutex::new(false));
    // }
    // sets.par_iter().enumerate().for_each(|(i, i_set)| {
    //     for j in i..n {
    //         let e = n * i + j;
    //         let j_set = sets.get(j).unwrap();
    //
    //         let mut conflict = false;
    //         for addr in j_set.iter() {
    //             conflict = conflict || i_set.contains(addr);
    //         }
    //         let mutex = edges.get(e).unwrap();
    //         let mut edge = mutex.lock().unwrap();
    //         *edge = conflict;
    //     }
    // });
    //
    // // let addresses: Vec<u64> = address_set.into_iter().collect();
    // // let elapsed = a.elapsed();
    // println!("Size of matrix: {:?}", edges.len());
    // println!("Took {:?}", elapsed);
    // println!();

    return elapsed;
    //endregion
}

fn try_parallel_unsafe(batch: &Vec<T>, rng: &mut StdRng, storage_size: usize) -> Duration {
    //region parallel unsafe -----------------------------------------------------------------------
    println!("Parallel (unsafe) generating access sets and computing conflict edges...");
    let n = batch.len();

    let mut sets = vec![
        tinyset::SetU64::with_capacity_and_max(2, storage_size as u64);
        n
    ];

    let a = Instant::now();
    // for (i, i_tx) in batch.iter().enumerate() {
    //     let mut i_set = sets.get_mut(i).unwrap();
    //     i_set.insert(i_tx.from);
    //     i_set.insert(i_tx.to);
    // }
    sets.par_iter_mut().enumerate().for_each(|(i, set)|{
        let tx = batch.get(i).unwrap();
        set.insert(tx.from);
        set.insert(tx.to);
    });
    let elapsed = a.elapsed();
    println!("Creating sets took {:?}", elapsed);

    // println!();
    // println!("Filling conflict matrix...");
    // #[derive(Copy, Clone)]
    // struct Ptr {
    //     ptr: *mut bool,
    // }
    // unsafe impl Sync for Ptr { }
    //
    // unsafe impl Send for Ptr { }
    // impl Ptr {
    //     unsafe fn or(&mut self, offset: usize, value: bool) {
    //         let mut edge = self.ptr.add(offset);
    //         *edge = *edge || value;
    //     }
    // }
    //
    // let mut edges = vec![false; n*n];
    //
    // unsafe {
    //
    //     let mut edges_unsafe = Ptr{ ptr: edges.as_mut_ptr() };
    //     sets.par_iter().enumerate().for_each(|(i, i_set)| {
    //         for j in i..n {
    //             let e = n * i + j;
    //             let j_set = sets.get(j).unwrap();
    //             for addr in j_set.iter() {
    //                 edges_unsafe.clone().or(e, i_set.contains(addr));
    //             }
    //         }
    //     });
    // }
    //
    // // let addresses: Vec<u64> = address_set.into_iter().collect();
    // let elapsed = a.elapsed();
    // println!("Filled noted some edges among {:?}", edges.len());
    // println!("Took {:?}", elapsed);
    // println!();
    return elapsed;
    //endregion
}

fn try_parallel_without_mutex(batch: &Vec<T>, rng: &mut StdRng, storage_size: usize) -> Duration {
    //region parallel (no mutex) -----------------------------------------------------------------------
    println!("Parallel (no mutex) generating access sets and computing conflict edges...");
    let n = batch.len();

    let mut sets = vec![
        tinyset::SetU64::with_capacity_and_max(2, storage_size as u64);
        n
    ];

    let a = Instant::now();
    // for (i, i_tx) in batch.iter().enumerate() {
    //     let mut i_set = sets.get_mut(i).unwrap();
    //     i_set.insert(i_tx.from);
    //     i_set.insert(i_tx.to);
    // }
    sets.par_iter_mut().enumerate().for_each(|(i, set)|{
        let tx = batch.get(i).unwrap();
        set.insert(tx.from);
        set.insert(tx.to);
    });
    let elapsed = a.elapsed();
    println!("Creating sets took {:?}", elapsed);

    // println!();
    // println!("Filling conflict matrix...");
    // let mut edges: Vec<Vec<bool>> = vec![
    //     vec![false; n]; n
    // ];
    // edges.par_iter_mut().enumerate().for_each(|(i, tx_column)| {
    //     let i_set = sets.get(i).unwrap();
    //     for j in 0..i {
    //         let j_set = sets.get(j).unwrap();
    //         let mut conflict = false;
    //         for addr in j_set.iter() {
    //             conflict = conflict || i_set.contains(addr);
    //         }
    //         tx_column[j] = conflict;
    //     }
    // });
    //
    // // let addresses: Vec<u64> = address_set.into_iter().collect();
    // let elapsed = a.elapsed();
    // println!("Height of Matrix {:?}", edges.len());
    // println!("Took {:?}", elapsed);
    // println!();

    return elapsed;
    //endregion
}

// fn profile_parallel_contract() -> Result<()> {
//     let mut vm = ParallelVM::new(4)?;
//     let batch_size = 65536;
//     let storage_size = 100 * batch_size;
//     let initial_balance = 10;
//
//     if let Data::NewContract(functions) = ExternalRequest::new_coin().data {
//         let mut storage = VmStorage::new(storage_size);
//         storage.set_storage(initial_balance);
//         let mut new_contract = Contract{
//             storage,
//             functions: functions.clone(),
//         };
//         vm.contracts.push(new_contract);
//
//         let mut rng = StdRng::seed_from_u64(10);
//
//         let batch = ExternalRequest::batch_with_conflicts(
//             storage_size,
//             batch_size,
//             0.0,
//             &mut rng
//         );
//
//         // let batch = vec!(
//         //     ExternalRequest::transfer(0, 1, 1),
//         //     ExternalRequest::transfer(1, 2, 2),
//         //     ExternalRequest::transfer(2, 3, 3),
//         //     ExternalRequest::transfer(3, 4, 4),
//         //     ExternalRequest::transfer(4, 5, 5),
//         // );
//
//         // println!("Accounts balance before execution: {:?}", vm.contracts[0].storage);
//         let a = Instant::now();
//         let _result = vm.execute(batch);
//         let duration = a.elapsed();
//         // println!("Accounts balance after execution: {:?}", vm.contracts[0].storage);
//         println!("Took {:?}", duration);
//     }
//
//     Ok(())
// }

// fn test_new_transactions() -> Result<()> {
//     let mut serial_vm = SerialVM::new(10)?;
//     serial_vm.set_account_balance(10);
//
//     let batch = vec!(
//         ExternalRequest::transfer(0, 1, 1),
//         ExternalRequest::transfer(1, 2, 2),
//         ExternalRequest::transfer(2, 3, 3),
//         ExternalRequest::transfer(3, 4, 4),
//         ExternalRequest::transfer(4, 5, 5),
//     );
//
//     println!("Accounts balance before execution: {:?}", serial_vm.accounts);
//     let _result = serial_vm.execute(batch);
//     println!("Accounts balance after execution: {:?}", serial_vm.accounts);
//
//     Ok(())
// }

// fn test_new_contracts() -> Result<()> {
//
//     let mut serial_vm = SerialVM::new(0)?;
//
//     let batch = vec!(ExternalRequest::new_coin());
//     let _result = serial_vm.execute(batch);
//     serial_vm.contracts[0].storage.content.resize(10, 10);
//
//     let batch = vec!(
//         ExternalRequest::call_contract(0, 0, 1, 1),
//         ExternalRequest::call_contract(1, 0, 2, 2),
//         ExternalRequest::call_contract(2, 0, 3, 3),
//         ExternalRequest::call_contract(3, 0, 4, 4),
//         ExternalRequest::call_contract(4, 0, 5, 5),
//     );
//
//     println!("Storage before execution: {:?}", serial_vm.contracts[0].storage);
//     let _result = serial_vm.execute(batch);
//     println!("Storage after execution: {:?}", serial_vm.contracts[0].storage);
//
//     Ok(())
// }

fn profile_old_tx(path: &str) -> Result<()> {
    let config = BenchmarkConfig::new(path)
        .context("Unable to create benchmark config")?;

    let batch_size = config.batch_sizes[0];
    let storage_size = batch_size * 100;
    // let nb_cores = config.nb_cores[0];
    let conflict_rate = config.workloads[0].as_str();

    let mut rng = match config.seed {
        Some(seed) => {
            StdRng::seed_from_u64(seed)
        },
        None => StdRng::seed_from_u64(rand::random())
    };

    let mut latency_exec = Duration::from_nanos(0);

    let mut serial_vm = VMa::new(storage_size)?;


    for _ in 0..config.repetitions {
        let mut batch = batch_with_conflicts_new_impl(
            storage_size,
            batch_size,
            0.0,    // TODO???
            &mut rng
        );
        serial_vm.set_storage(200);

        let b =  Instant::now();
        let _ = serial_vm.execute(batch);
        let duration = b.elapsed();
        latency_exec = latency_exec.add(duration);
    }

    println!("old tx:");
    println!("Average latency: {:?}", latency_exec.div(config.repetitions as u32));
    let avg = latency_exec.div(config.repetitions as u32);
    println!("Throughput = {} tx/µs", batch_size as u128/avg.as_micros());
    println!();

    Ok(())
}

// fn profile_new_tx(path: &str) -> Result<()> {
//     let config = BenchmarkConfig::new(path)
//         .context("Unable to create benchmark config")?;
//
//     let batch_size = config.batch_sizes[0];
//     let storage_size = batch_size * 100;
//     // let nb_cores = config.nb_cores[0];
//     let conflict_rate = config.conflict_rates[0];
//
//     let mut rng = match config.seed {
//         Some(seed) => {
//             StdRng::seed_from_u64(seed)
//         },
//         None => StdRng::seed_from_u64(rand::random())
//     };
//
//     let mut latency_exec = Duration::from_nanos(0);
//
//     let mut serial_vm = SerialVM::new(storage_size)?;
//
//     for _ in 0..config.repetitions {
//         serial_vm.set_account_balance(200);
//         let batch = ExternalRequest::batch_with_conflicts(
//             storage_size,
//             batch_size,
//             conflict_rate,
//             &mut rng
//         );
//         let b =  Instant::now();
//         serial_vm.execute(batch)?;
//         latency_exec = latency_exec.add(b.elapsed());
//     }
//
//     println!("new tx: native");
//     println!("Average latency: {:?}", latency_exec.div(config.repetitions as u32));
//     let avg = latency_exec.div(config.repetitions as u32);
//     println!("Throughput = {} tx/µs", batch_size as u128/avg.as_micros());
//     println!();
//
//     Ok(())
// }

// fn profile_new_contract(path: &str) -> Result<()> {
//     let config = BenchmarkConfig::new(path)
//         .context("Unable to create benchmark config")?;
//
//     let batch_size = config.batch_sizes[0];
//     let storage_size = batch_size * 100;
//     // let nb_cores = config.nb_cores[0];
//     let conflict_rate = config.conflict_rates[0];
//
//     let mut rng = match config.seed {
//         Some(seed) => {
//             StdRng::seed_from_u64(seed)
//         },
//         None => StdRng::seed_from_u64(rand::random())
//     };
//
//     let mut latency_exec = Duration::from_nanos(0);
//     let mut serial_vm = SerialVM::new(storage_size)?;
//     let batch = vec!(ExternalRequest::new_coin());
//     let _result = serial_vm.execute(batch);
//     serial_vm.contracts[0].storage.content.resize(storage_size, 0);
//
//     for _ in 0..config.repetitions {
//         serial_vm.contracts[0].storage.set_storage(200);
//         let mut batch = ExternalRequest::batch_with_conflicts_contract(
//             storage_size,
//             batch_size,
//             conflict_rate,
//             &mut rng
//         );
//         let b =  Instant::now();
//         serial_vm.execute(batch)?;
//         latency_exec = latency_exec.add(b.elapsed());
//     }
//
//     println!("new tx: contract");
//     println!("Average latency: {:?}", latency_exec.div(config.repetitions as u32));
//     let avg = latency_exec.div(config.repetitions as u32);
//     println!("Throughput = {} tx/µs", batch_size as u128/avg.as_micros());
//     println!();
//
//     Ok(())
// }

#[allow(dead_code)]
fn profiling(path: &str) -> Result<()> {

    let config = BenchmarkConfig::new(path)
        .context("Unable to create benchmark config")?;

    let batch_size = config.batch_sizes[0];
    let storage_size = batch_size * 100;
    let nb_cores = config.nb_executors[0];
    let conflict_rate = config.workloads[0].as_str();

    let mut rng = match config.seed {
        Some(seed) => {
            StdRng::seed_from_u64(seed)
        },
        None => StdRng::seed_from_u64(rand::random())
    };

    let mut _initial_batch: Vec<BasicTransaction> = batch_with_conflicts_new_impl(
        storage_size,
        batch_size,
        0.0, // TODO???
        &mut rng
    );
    // let mut initial_batch: Vec<Transaction> = batch_with_conflicts(
    //     batch_size,
    //     conflict_rate,
    //     &mut rng
    // );
    let mut backlog: Vec<BasicTransaction> = Vec::with_capacity(_initial_batch.len());

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