#![allow(unused_imports)]
extern crate anyhow;
extern crate either;
extern crate hwloc;
extern crate tokio;

use futures::future::BoxFuture;
use itertools::Itertools;
use voracious_radix_sort::{RadixSort};
use std::collections::{BTreeSet, HashSet};
use std::ops::{Add, Div};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use thincollections::thin_set::ThinSet;
use nohash_hasher::IntSet;
use rayon::prelude::*;
use anyhow::{anyhow, Context, Result};
use bloomfilter::Bloom;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
// use core_affinity;

use testbench::benchmark::benchmarking;
use testbench::config::{BenchmarkConfig, ConfigFile};
use testbench::transaction::{Transaction, TransactionAddress};
use testbench::utils::{batch_with_conflicts, batch_with_conflicts_new_impl};
use testbench::vm::{ExecutionResult, Executor};
use testbench::vm_a::{SerialVM, VMa};
use testbench::vm_c::{ParallelVM, VMc};
use testbench::vm_utils::{assign_workers, UNASSIGNED, VmStorage};
use testbench::wip::{AccessType, assign_workers_new_impl, assign_workers_new_impl_2, Contract, Data, ExternalRequest, Param};
use testbench::worker_implementation::WorkerC;


type A = u64;
type Set = IntSet<u64>;
#[derive(Copy, Clone, Debug)]
struct T {
    from: A,
    to: A,
}

#[tokio::main]
async fn main() -> Result<()>{
    println!("Hello, world!");

    // let _ = BasicWorkload::run(config, 1).await;
    // let _ = ContentionWorkload::run(config, 1).await;
    // let _ = TransactionLoop::run(config, 1).await;
    // let _ = ConflictWorkload::run(config, 1);

    // benchmarking("benchmark_config.json")?;
    // profiling("benchmark_config.json")?;

    // profile_old_tx("benchmark_config.json")?;
    // profile_new_tx("benchmark_config.json")?;
    // profile_new_contract("benchmark_config.json")?;

    let mut rng = StdRng::seed_from_u64(10);
    let batch_size = 65536;
    let storage_size = 100 * batch_size;

    let total = Instant::now();
    print!("Creating batch of size {}... ", batch_size);
    let a = Instant::now();
    let batch: Vec<T> = batch_with_conflicts_new_impl(
        storage_size,
        batch_size,
        0.5,
        &mut rng
    ).par_iter().map(|tx| T { from: tx.from, to: tx.to}).collect();
    println!("Took {:?}", a.elapsed());
    println!();

    let small_batch_size = 2048;
    let small_storage_size = 100 * batch_size;
    print!("Creating batch of size {}... ", small_batch_size);
    let a = Instant::now();
    let small_batch: Vec<T> = batch_with_conflicts_new_impl(
        small_storage_size,
        small_batch_size,
        0.5,
        &mut rng
    ).par_iter().map(|tx| T { from: tx.from, to: tx.to}).collect();

    println!("Took {:?}", a.elapsed());
    println!();

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

    println!("=====================================================");
    println!();

    try_scheduling_sequential(&batch, storage_size);

    // println!("=====================================================");
    // println!();
    // try_scheduling_parallel(&batch, storage_size);

    // println!("=====================================================");
    // println!();
    // try_merge_sort(&batch, storage_size).await;

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
    let depth_limit = 1;
    println!("Merge with depth {}", depth_limit);
    let start = Instant::now();
    let res = merge_sort(batch.as_slice(), storage_size, 0, depth_limit);
    let duration = start.elapsed();
    println!("Need {} rounds", res.len());
    println!("Took {:?}", duration);
    for (round, (_, scheduled_tx)) in res.iter().enumerate() {
        println!("\tRound {} has {} txs", round, scheduled_tx.len());
    }

//     let _ = tokio::spawn(async move {
//         let start = Instant::now();
//         let res = merge_sort_async(batch.as_slice(), storage_size, 0, 1).await.unwrap();
//         // recursive(root_path, tx, ext).await.unwrap();
//         let duration = start.elapsed();
//         println!("Need {} rounds", res.len());
//         println!("Took {:?}", duration);
//     }).await;
}

fn merge_sort(chunk: &[T], storage_size: usize, depth: usize, depth_limit: usize) -> Vec<(tinyset::SetU64, Vec<&T>)> {

    return if depth >= depth_limit {
        let start = Instant::now();
        let set_capacity = chunk.len() * 2;
        // let set_capacity = 65536 * 2;
        let mut rounds = vec![(
            tinyset::SetU64::with_capacity_and_max(set_capacity, storage_size as u64),
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
            let new_round_addr = tinyset::SetU64::with_capacity_and_max(set_capacity, storage_size as u64);
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
        );;

        let mut left_round_index = 0;

        let mut additional_rounds = vec!();
        'right: for (mut right_addr, mut right_tx) in right.into_iter() {
            'left: for round in left_round_index..left.len() {
                let left_addr = &mut left[round].0;

                'addr: for addr in right_addr.iter() {
                    if left_addr.contains(addr) {
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

fn profile_parallel_contract() -> Result<()> {
    let mut vm = ParallelVM::new(4)?;
    let batch_size = 65536;
    let storage_size = 100 * batch_size;
    let initial_balance = 10;

    if let Data::NewContract(functions) = ExternalRequest::new_coin().data {
        let mut storage = VmStorage::new(storage_size);
        storage.set_storage(initial_balance);
        let mut new_contract = Contract{
            storage,
            functions: functions.clone(),
        };
        vm.contracts.push(new_contract);

        let mut rng = StdRng::seed_from_u64(10);

        let batch = ExternalRequest::batch_with_conflicts(
            storage_size,
            batch_size,
            0.0,
            &mut rng
        );

        // let batch = vec!(
        //     ExternalRequest::transfer(0, 1, 1),
        //     ExternalRequest::transfer(1, 2, 2),
        //     ExternalRequest::transfer(2, 3, 3),
        //     ExternalRequest::transfer(3, 4, 4),
        //     ExternalRequest::transfer(4, 5, 5),
        // );

        // println!("Accounts balance before execution: {:?}", vm.contracts[0].storage);
        let a = Instant::now();
        let _result = vm.execute(batch);
        let duration = a.elapsed();
        // println!("Accounts balance after execution: {:?}", vm.contracts[0].storage);
        println!("Took {:?}", duration);
    }

    Ok(())
}

fn test_new_transactions() -> Result<()> {
    let mut serial_vm = SerialVM::new(10)?;
    serial_vm.set_account_balance(10);

    let batch = vec!(
        ExternalRequest::transfer(0, 1, 1),
        ExternalRequest::transfer(1, 2, 2),
        ExternalRequest::transfer(2, 3, 3),
        ExternalRequest::transfer(3, 4, 4),
        ExternalRequest::transfer(4, 5, 5),
    );

    println!("Accounts balance before execution: {:?}", serial_vm.accounts);
    let _result = serial_vm.execute(batch);
    println!("Accounts balance after execution: {:?}", serial_vm.accounts);

    Ok(())
}

fn test_new_contracts() -> Result<()> {

    let mut serial_vm = SerialVM::new(0)?;

    let batch = vec!(ExternalRequest::new_coin());
    let _result = serial_vm.execute(batch);
    serial_vm.contracts[0].storage.content.resize(10, 10);

    let batch = vec!(
        ExternalRequest::call_contract(0, 0, 1, 1),
        ExternalRequest::call_contract(1, 0, 2, 2),
        ExternalRequest::call_contract(2, 0, 3, 3),
        ExternalRequest::call_contract(3, 0, 4, 4),
        ExternalRequest::call_contract(4, 0, 5, 5),
    );

    println!("Storage before execution: {:?}", serial_vm.contracts[0].storage);
    let _result = serial_vm.execute(batch);
    println!("Storage after execution: {:?}", serial_vm.contracts[0].storage);

    Ok(())
}

fn profile_old_tx(path: &str) -> Result<()> {
    let config = BenchmarkConfig::new(path)
        .context("Unable to create benchmark config")?;

    let batch_size = config.batch_sizes[0];
    let storage_size = batch_size * 100;
    // let nb_cores = config.nb_cores[0];
    let conflict_rate = config.conflict_rates[0];

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
            conflict_rate,
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

fn profile_new_tx(path: &str) -> Result<()> {
    let config = BenchmarkConfig::new(path)
        .context("Unable to create benchmark config")?;

    let batch_size = config.batch_sizes[0];
    let storage_size = batch_size * 100;
    // let nb_cores = config.nb_cores[0];
    let conflict_rate = config.conflict_rates[0];

    let mut rng = match config.seed {
        Some(seed) => {
            StdRng::seed_from_u64(seed)
        },
        None => StdRng::seed_from_u64(rand::random())
    };

    let mut latency_exec = Duration::from_nanos(0);

    let mut serial_vm = SerialVM::new(storage_size)?;

    for _ in 0..config.repetitions {
        serial_vm.set_account_balance(200);
        let batch = ExternalRequest::batch_with_conflicts(
            storage_size,
            batch_size,
            conflict_rate,
            &mut rng
        );
        let b =  Instant::now();
        serial_vm.execute(batch)?;
        latency_exec = latency_exec.add(b.elapsed());
    }

    println!("new tx: native");
    println!("Average latency: {:?}", latency_exec.div(config.repetitions as u32));
    let avg = latency_exec.div(config.repetitions as u32);
    println!("Throughput = {} tx/µs", batch_size as u128/avg.as_micros());
    println!();

    Ok(())
}

fn profile_new_contract(path: &str) -> Result<()> {
    let config = BenchmarkConfig::new(path)
        .context("Unable to create benchmark config")?;

    let batch_size = config.batch_sizes[0];
    let storage_size = batch_size * 100;
    // let nb_cores = config.nb_cores[0];
    let conflict_rate = config.conflict_rates[0];

    let mut rng = match config.seed {
        Some(seed) => {
            StdRng::seed_from_u64(seed)
        },
        None => StdRng::seed_from_u64(rand::random())
    };

    let mut latency_exec = Duration::from_nanos(0);
    let mut serial_vm = SerialVM::new(storage_size)?;
    let batch = vec!(ExternalRequest::new_coin());
    let _result = serial_vm.execute(batch);
    serial_vm.contracts[0].storage.content.resize(storage_size, 0);

    for _ in 0..config.repetitions {
        serial_vm.contracts[0].storage.set_storage(200);
        let mut batch = ExternalRequest::batch_with_conflicts_contract(
            storage_size,
            batch_size,
            conflict_rate,
            &mut rng
        );
        let b =  Instant::now();
        serial_vm.execute(batch)?;
        latency_exec = latency_exec.add(b.elapsed());
    }

    println!("new tx: contract");
    println!("Average latency: {:?}", latency_exec.div(config.repetitions as u32));
    let avg = latency_exec.div(config.repetitions as u32);
    println!("Throughput = {} tx/µs", batch_size as u128/avg.as_micros());
    println!();

    Ok(())
}

#[allow(dead_code)]
fn profiling(path: &str) -> Result<()> {

    let config = BenchmarkConfig::new(path)
        .context("Unable to create benchmark config")?;

    let batch_size = config.batch_sizes[0];
    let storage_size = batch_size * 100;
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