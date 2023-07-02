use std::mem;
use std::cmp::max;
use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::BuildHasherDefault;
use std::slice::Iter;
use std::sync::Arc;

// use hwloc2::{Topology, TopologyObject, ObjectType, CpuBindFlags};
use ahash::AHasher;
use anyhow::anyhow;
use core_affinity::CoreId;
use hashbrown::HashMap as BrownMap;
use itertools::Itertools;
use nohash_hasher::{BuildNoHashHasher, NoHashHasher};
use rand::{RngCore, SeedableRng};
use rand::distributions::{Distribution, WeightedIndex};
use rand::prelude::{SliceRandom, StdRng};
use tabled::{Table, Tabled};
use thincollections::thin_map::ThinMap;
use tokio::time::{Duration, Instant};

use crate::benchmark::WorkloadUtils;
use crate::contract::{AccessPattern, AccessType, AtomicFunction, FunctionParameter, SenderAddress, StaticAddress, Transaction, TransactionType};
use crate::contract::AtomicFunction::PieceDHashMap;
use crate::contract::FunctionResult::Another;
use crate::d_hash_map::{DHashMap, PiecedOperation};
use crate::utils::mean_ci;
use crate::vm_utils::{AddressSet, Schedule, Scheduling, SharedStorage, VmStorage};
use crate::wip::Word;

//region Measurements ------------------------------------------------------------------------------
#[derive(Clone)]
pub struct Measurement {
    execution_core: usize,
    memory_core: usize,
    array_size_bytes: usize,
    mean_latency: Duration,
    ci_mean_latency: Duration,
    nb_iterations: usize,
    nb_op_per_iteration: usize,
    total_duration: Duration,
}

impl Measurement {
    fn new(execution_core: usize,
           memory_core: usize,
           array_size_bytes: usize,
           latencies: &Vec<Duration>,
           total_duration: Duration,
            nb_operations: usize
    ) -> Self {
        let (mean_latency, ci_mean_latency) = mean_ci(latencies);
        Self {
            execution_core,
            memory_core,
            array_size_bytes,
            mean_latency,
            ci_mean_latency,
            nb_iterations: latencies.len(),
            nb_op_per_iteration: nb_operations,
            total_duration,
        }
    }

    fn to_json(&self) -> String {
        let output = vec![
            "{".to_string(),
            format!("execution_core: {},", self.execution_core),
            format!("memory_core: {},", self.memory_core),
            format!("array_size_bytes: {},", self.array_size_bytes),
            format!("mean_latency_ns_per_op: {:.3},", (self.mean_latency.as_nanos() as f64) / (self.nb_op_per_iteration as f64)),
            format!("ci_mean_latency_ns_per_op: {:.3},", (self.ci_mean_latency.as_nanos() as f64) / (self.nb_op_per_iteration as f64)),
            format!("nb_iterations: {},", self.nb_iterations),
            format!("total_duration_ms: {},", self.total_duration.as_millis()),
            "},".to_string()
        ];

        output.join(" ")
    }
}

#[derive(Tabled, Clone)]
struct SizeLatencyMeasurement {
    array_size: String,
    mean: String,
    ci: String,
    nb_iterations: usize,
    total_duration: String
}

impl SizeLatencyMeasurement {
    fn from(measurement: &Measurement) -> Self {
        Self {
            array_size: adapt_unit(measurement.array_size_bytes),
            mean: format!("{:.3?}", measurement.mean_latency),
            ci: format!("{:.3?}", measurement.ci_mean_latency),
            nb_iterations: measurement.nb_iterations,
            total_duration: format!("{:.3?}", measurement.total_duration)
        }
    }
}

#[derive(Tabled, Clone)]
struct CoreLatencyMeasurement {
    core: String,
    mean: String,
    ci: String,
    nb_iterations: usize,
    total_duration: String
}

impl CoreLatencyMeasurement {
    fn from(measurement: &Measurement) -> Self {
        Self {
            core: format!("Core#{}", measurement.memory_core),
            mean: format!("{:.3?}", measurement.mean_latency),
            ci: format!("{:.3?}", measurement.ci_mean_latency),
            nb_iterations: measurement.nb_iterations,
            total_duration: format!("{:.3?}", measurement.total_duration)
        }
    }
}
//endregion

//region Microbenchmarks
const ENTRY_SIZE: usize = 8;
const NB_BUCKETS: usize = 1024;
const BUCKET_CAPACITY_ELEMS: usize = 64;

//region Scheduling quality (without parallelism)
pub async fn micro_scheduling_async() {
    eprintln!("Async version =================================");
    micro_scheduling();
}

pub fn micro_scheduling() {
    let batch_size: usize = 65536;
    let storage_size: usize = 100 * batch_size;
    let seed: u64 = 10;
    // let fractions = vec![1, 2, 4, 8, 16, 24, 32];
    // let executors = vec![4, 8, 16, 24, 32];
    let fractions = vec![1, 2, 4];
    let executors = vec![4, 8, 16];
    let nb_repetitions = 100;

    let mut rng = StdRng::seed_from_u64(seed);
    let mut measurements = vec![Duration::from_secs(0); nb_repetitions];

    // Transfer 0.0 --------------------------------------------------------------------------------
    let conflict_rate = 0.0;
    let mut batch_0 = WorkloadUtils::transfer_pairs(storage_size, batch_size, conflict_rate, &mut rng)
        .iter().enumerate()
        .map(|(tx_index, pair)| {
            Transaction {
                sender: pair.0 as SenderAddress,
                function: AtomicFunction::Transfer,
                tx_index,
                addresses: [pair.0, pair.1],
                // Transfer loop
                // addresses: [tx_index as StaticAddress, 1 + tx_index as StaticAddress],
                params: [2],
            }
        }).collect_vec();


    // Transfer 0.5 --------------------------------------------------------------------------------
    let conflict_rate = 0.5;
    let mut batch_1 = WorkloadUtils::transfer_pairs(storage_size, batch_size, conflict_rate, &mut rng)
        .iter().enumerate()
        .map(|(tx_index, pair)| {
            Transaction {
                sender: pair.0 as SenderAddress,
                function: AtomicFunction::Transfer,
                tx_index,
                addresses: [pair.0, pair.1],
                // Transfer loop
                // addresses: [tx_index as StaticAddress, 1 + tx_index as StaticAddress],
                params: [2],
            }
        }).collect_vec();

    // DHashMap 10% ------------------------------------------------------------------------------------
    let weights = [0.7, 0.05, 0.05, 0.2];
    let (batch_2, mut hashmap_storage) = dhashmap_batch(batch_size, storage_size, &mut rng, weights);

    // DHashMap 50% ------------------------------------------------------------------------------------
    let weights = [0.25, 0.25, 0.25, 0.25];
    let (batch_3, mut hashmap_storage) = dhashmap_batch(batch_size, storage_size, &mut rng, weights);

    // Fibonacci -----------------------------------------------------------------------------------
    let mut batch_4 = WorkloadUtils::transfer_pairs(storage_size, batch_size, conflict_rate, &mut rng)
        .iter().enumerate()
        .map(|(tx_index, pair)| {
            Transaction {
                sender: pair.0 as SenderAddress,
                function: AtomicFunction::Fibonacci,
                tx_index,
                addresses: [],
                params: [2],
            }
        }).collect_vec();

    let mut storage = VmStorage::new(storage_size);
    let process_all_chunks = false;
    let chunk_config = if process_all_chunks {
        |fraction: usize, batch_size: usize| { (fraction, batch_size) }
    } else {
        |fraction: usize, batch_size: usize| { (1, batch_size/fraction + 1) }
    };

    println!("// Transfer(0.0): =======================================================================");
    measure_latency_vs_parallelism::<2, 1>(
        false,
        "Transfer(0.0)",
        &batch_0,
        &fractions,
        &executors,
        nb_repetitions,
        [1, 1, 1],
        &mut measurements,
        &mut storage,
        &chunk_config
    );
    println!();

    println!("// Transfer(0.5): =======================================================================");
    measure_latency_vs_parallelism::<2, 1>(
        false,
        "Transfer(0.5)",
        &batch_1,
        &fractions,
        &executors,
        nb_repetitions,
        [3, 3, 3],
        &mut measurements,
        &mut storage,
        &chunk_config
    );

    println!("// DHashMap(10): ============================================================================");
    measure_latency_vs_parallelism::<5, 8>(
        true,
        "DHashMap(10)",
        &batch_3,
        &fractions,
        &executors,
        nb_repetitions,
        [5, 5, 1],
        &mut measurements,
        &mut hashmap_storage,
        &chunk_config
    );

    println!("// DHashMap(50): ============================================================================");
    measure_latency_vs_parallelism::<5, 8>(
        true,
        "DHashMap(50)",
        &batch_2,
        &fractions,
        &executors,
        nb_repetitions,
        [5, 5, 1],
        &mut measurements,
        &mut hashmap_storage,
        &chunk_config
    );

    println!("// Fibonacci: ============================================================================");
    measure_latency_vs_parallelism::<0, 1>(
        false,
        "Fibonacci",
        &batch_4,
        &fractions,
        &executors,
        nb_repetitions,
        [1, 1, 1],
        &mut measurements,
        &mut storage,
        &chunk_config
    );

    if process_all_chunks {
        println!("Latency = schedule whole batch in chunks");
    } else {
        println!("Latency = schedule standalone chunks");
    }
}

fn measure_latency_vs_parallelism<const A: usize, const P: usize>(
    is_hashmap: bool,
    workload: &str,
    batch: &Vec<Transaction<A, P>>,
    fractions: &Vec<usize>,
    executors: &Vec<usize>,
    nb_repetitions: usize,
    max_nb_iters: [usize; 3],
    measurements: &mut Vec<Duration>,
    storage: &mut VmStorage,
    chunk_config: &dyn Fn(usize, usize) -> (usize, usize)
)
{
    let mut init_storage = |s: &mut VmStorage| {
        if is_hashmap {
            DHashMap::init::<ENTRY_SIZE>(&mut s.content, NB_BUCKETS, BUCKET_CAPACITY_ELEMS);
        } else {
            s.set_storage(200 * nb_repetitions as Word);
        }
    };

    // Basic scheduling ----------------------------------------------------------------------------
    micro_read_only_scheduling(workload, batch, &fractions, nb_repetitions, measurements, &chunk_config);
    println!();

    // Address scheduling --------------------------------------------------------------------------
    for nb_iters in 1..max_nb_iters[0]+1 {
        init_storage(storage);
        micro_address_scheduling(workload, batch, &fractions, nb_repetitions, measurements, storage.get_shared(), Some(nb_iters), &chunk_config);
    }
    // init_storage(storage);
    // micro_address_scheduling(workload, batch, &fractions, nb_repetitions, measurements, storage.get_shared(), None, nb_chunks);
    println!();

    // Read/Write scheduling -----------------------------------------------------------------------
    for nb_iters in 1..max_nb_iters[1]+1 {
        init_storage(storage);
        micro_basic_scheduling(workload, batch, &fractions, nb_repetitions, measurements, storage.get_shared(), Some(nb_iters), &chunk_config);
    }
    // init_storage(storage);
    // micro_read_write_scheduling(workload, batch, &fractions, nb_repetitions, measurements, storage.get_shared(), None, &nb_chunks);
    println!();

    // Assignment scheduling -----------------------------------------------------------------------
    for nb_executors in executors.iter() {
        for nb_iters in 1..max_nb_iters[2]+1 {
            init_storage(storage);
            micro_advanced_scheduling(workload, batch, &fractions, *nb_executors, nb_repetitions, measurements, storage.get_shared(), Some(nb_iters), &chunk_config);
        }
    }
    // init_storage(storage);
    // micro_assign_scheduling(workload, batch, &fractions, executors, nb_repetitions, measurements, storage.get_shared(), None, &nb_chunks);
    println!();
}

fn micro_read_only_scheduling<const A: usize, const P: usize>(
    workload: &str,
    batch: &Vec<Transaction<A, P>>,
    fractions: &Vec<usize>,
    nb_repetitions: usize,
    measurements: &mut Vec<Duration>,
    chunk_config: &dyn Fn(usize, usize) -> (usize, usize)
)
{
    let batch_size = batch.len();
    let mut results = Vec::with_capacity(fractions.len());

    let mut read_only = Vec::with_capacity(batch_size);
    let mut postponed = Vec::with_capacity(batch_size);
    for fraction in fractions.iter() {
        let chunk_size = batch_size/fraction + 1;
        let (nb_chunks, total_size) = chunk_config(*fraction, batch_size);
        let mut nb_parallel = 0.0;
        for i in 0..nb_repetitions {
            measurements[i] = Duration::from_secs(0);
            nb_parallel = 0.0;
            let mut backlog = batch.iter();
            for _ in 0..nb_chunks {
                let chunk = Vec::from_iter(backlog.by_ref().take(chunk_size).cloned());
                chunk.len();
                let start = Instant::now();
                ReadOnlyScheduling::schedule::<A, P>(&chunk, &mut read_only, &mut postponed);
                measurements[i] += start.elapsed();
                nb_parallel += read_only.len() as f64;
                // println!("\tBasic: Scheduled: {} txs, read_only: {} txs, postponed: {} txs", scheduled.len(), read_only.len(), postponed.len());
                postponed.truncate(0);
                read_only.truncate(0);
            }
        }

        let mean_latency = mean_ci(&measurements).0;
        let parallelism = nb_parallel/(total_size as f64);
        results.push((parallelism, mean_latency.as_micros(), total_size));

        println!("{{\
        workload: '{workload:}', \
        scheduling: 'read_only', \
        size: {chunk_size:}, \
        parallelism: {parallelism:.3}, \
        latency: {:.3}, \
        iter: 1 \
        }},",
                 mean_latency.as_micros()
        );
    }

    // println!("Basic scheduling: {:.3?}", results);
}

fn micro_address_scheduling<const A: usize, const P: usize>(
    workload: &str,
    batch: &Vec<Transaction<A, P>>,
    fractions: &Vec<usize>,
    nb_repetitions: usize,
    measurements: &mut Vec<Duration>,
    storage: SharedStorage,
    max_nb_iterations: Option<usize>,
    chunk_config: &dyn Fn(usize, usize) -> (usize, usize)
)
{
    let batch_size = batch.len();
    let mut results = Vec::with_capacity(fractions.len());
    let max = max_nb_iterations.unwrap_or(usize::MAX);

    let mut scheduled = Vec::with_capacity(batch_size);
    let mut read_only = Vec::with_capacity(batch_size);
    let mut postponed = Vec::with_capacity(batch_size);
    for fraction in fractions.iter() {
        let mut current_iter = 1;
        let chunk_size = batch_size/fraction + 1;
        let (nb_chunks, total_size) = chunk_config(*fraction, batch_size);
        let mut nb_parallel = 0.0;
        let mut working_set = AddressSet::with_capacity(2 * A * chunk_size);

        for i in 0..nb_repetitions {
            nb_parallel = 0.0;
            current_iter = 1;
            let mut backlog = batch.iter();
            let mut new_txs = Vec::with_capacity(chunk_size);
            let mut duration = Duration::from_secs(0);

            for _ in 0..nb_chunks {
                let mut chunk = Vec::from_iter(backlog.by_ref().take(chunk_size).cloned());
                while !chunk.is_empty() {
                    let start = Instant::now();

                    working_set.clear();

                    AddressScheduling::schedule::<A, P>(&chunk, &mut scheduled, &mut read_only, &mut postponed, &mut working_set);
                    duration += start.elapsed();

                    nb_parallel += (scheduled.len() + read_only.len()) as f64;
                    // println!("Iter {}: {} (+ {})", iters, nb_parallel, scheduled.len() + read_only.len());

                    execute_iter(
                        read_only.iter(),
                        &chunk,
                        &mut new_txs,
                        storage
                    );
                    execute_iter(
                        scheduled.iter(),
                        &chunk,
                        &mut new_txs,
                        storage
                    );
                    // Add postponed back into chunk
                    for tx_index in postponed.iter() {
                        let tx = chunk[*tx_index].clone();
                        new_txs.push(tx);
                    }

                    mem::swap(&mut chunk, &mut new_txs);
                    new_txs.truncate(0);

                    scheduled.truncate(0);
                    postponed.truncate(0);
                    read_only.truncate(0);

                    if current_iter >= max {
                        break;
                    }

                    current_iter += 1;
                }
            }
            measurements[i] = duration;
        }

        let mean_latency = mean_ci(&measurements).0;
        let parallelism = nb_parallel/(total_size as f64);
        results.push((parallelism, mean_latency.as_micros(), total_size, current_iter));
        println!("{{\
        workload: '{workload:}', \
        scheduling: 'address', \
        size: {chunk_size:}, \
        parallelism: {parallelism:.3}, \
        latency: {:.3}, \
        iter: {current_iter:} \
        }},",
                 mean_latency.as_micros()
        );
    }

    // println!("Address scheduling: {:.3?}", results);
}

fn micro_basic_scheduling<const A: usize, const P: usize>(
    workload: &str,
    batch: &Vec<Transaction<A, P>>,
    fractions: &Vec<usize>,
    nb_repetitions: usize,
    measurements: &mut Vec<Duration>,
    storage: SharedStorage,
    max_nb_iterations: Option<usize>,
    chunk_config: &dyn Fn(usize, usize) -> (usize, usize)
)
{
    let batch_size = batch.len();
    let mut results = Vec::with_capacity(fractions.len());
    let max = max_nb_iterations.unwrap_or(usize::MAX);

    let mut scheduled = Vec::with_capacity(batch_size);
    let mut read_only = Vec::with_capacity(batch_size);
    let mut postponed = Vec::with_capacity(batch_size);

    for fraction in fractions.iter() {
        let mut current_iter = 1;
        let chunk_size = batch_size/fraction + 1;
        let (nb_chunks, total_size) = chunk_config(*fraction, batch_size);
        let mut nb_parallel = 0.0;
        let mut address_map = HashMap::with_capacity_and_hasher(2 * A * chunk_size, BuildHasherDefault::default());

        for i in 0..nb_repetitions {
            nb_parallel = 0.0;
            current_iter = 1;
            let mut backlog = batch.iter();
            let mut new_txs = Vec::with_capacity(chunk_size);
            let mut duration = Duration::from_secs(0);

            for _ in 0..nb_chunks {
                let mut chunk = Vec::from_iter(backlog.by_ref().take(chunk_size).cloned());
                while !chunk.is_empty() {
                    let transactions = Arc::new(chunk);
                    let mut indices = (0..transactions.len()).collect_vec();
                    let start = Instant::now();

                    address_map.clear();

                    BasicScheduling::schedule_chunk_new::<A, P>(
                        &transactions,
                        &mut indices,
                        &mut scheduled,
                        &mut postponed,
                        &mut address_map,
                        &mut read_only
                    );
                    duration += start.elapsed();

                    chunk = Arc::try_unwrap(transactions).unwrap();
                    nb_parallel += (scheduled.len() + read_only.len()) as f64;
                    // println!("\tMedium: Scheduled: {} txs, read_only: {} txs, postponed: {} txs", scheduled.len(), read_only.len(), postponed.len());
                    // println!("Iter {}: {} (+ {})", iters, nb_parallel, scheduled.len() + read_only.len());
                    execute_iter(
                        read_only.iter(),
                        &chunk,
                        &mut new_txs,
                        storage
                    );
                    execute_iter(
                        scheduled.iter(),
                        &chunk,
                        &mut new_txs,
                        storage
                    );

                    // Add postponed back into chunk
                    for tx_index in postponed.iter() {
                        let tx = chunk[*tx_index].clone();
                        new_txs.push(tx);
                    }

                    mem::swap(&mut chunk, &mut new_txs);
                    new_txs.truncate(0);

                    scheduled.truncate(0);
                    postponed.truncate(0);
                    read_only.truncate(0);

                    if current_iter >= max {
                        break;
                    }

                    current_iter += 1;
                }
            }
            measurements[i] = duration;
        }

        let mean_latency = mean_ci(&measurements).0;
        let parallelism = nb_parallel/(total_size as f64);
        results.push((parallelism, mean_latency.as_micros(), total_size, current_iter));

        println!("{{\
        workload: '{workload:}', \
        scheduling: 'basic', \
        size: {chunk_size:}, \
        parallelism: {parallelism:.3}, \
        latency: {:.3}, \
        iter: {current_iter:} \
        }},",
                 mean_latency.as_micros()
        );
    }

    // println!("Read/Write scheduling: {:.3?}", results);
}

fn micro_advanced_scheduling<const A: usize, const P: usize>(
    workload: &str,
    batch: &Vec<Transaction<A, P>>,
    fractions: &Vec<usize>,
    nb_executors: usize,
    nb_repetitions: usize,
    measurements: &mut Vec<Duration>,
    storage: SharedStorage,
    max_nb_iterations: Option<usize>,
    chunk_config: &dyn Fn(usize, usize) -> (usize, usize)
)
{
    let batch_size = batch.len();
    let mut results = Vec::with_capacity(fractions.len());
    let max = max_nb_iterations.unwrap_or(usize::MAX);

    let mut new_reads = Vec::with_capacity(A * batch_size);
    let mut new_writes = Vec::with_capacity(A * batch_size);

    let mut schedule = Schedule::new(batch_size, nb_executors);
    for fraction in fractions.iter() {
        let mut current_iter = 1;
        let chunk_size = batch_size/fraction + 1;
        let (nb_chunks, total_size) = chunk_config(*fraction, batch_size);
        let mut nb_parallel = 0.0;
        let mut address_map = HashMap::with_capacity_and_hasher(2 * A * chunk_size, BuildHasherDefault::default());

        for i in 0..nb_repetitions {
            nb_parallel = 0.0;
            current_iter = 1;
            let mut backlog = batch.iter();
            let mut new_txs = Vec::with_capacity(chunk_size);
            let mut duration = Duration::from_secs(0);
            schedule.clear();
            for _ in 0..nb_chunks {
                schedule.transactions.extend(backlog.by_ref().take(chunk_size).cloned().collect_vec());
                while !schedule.transactions.is_empty() {
                    let start = Instant::now();
                    // // Those are reset by schedule_chunk_assign
                    // new_reads.truncate(0);
                    // new_writes.truncate(0);
                    // address_map.clear();

                    Scheduling::schedule_chunk_assign::<A, P>(
                        &mut address_map,
                        &mut new_reads,
                        &mut new_writes,
                        &mut schedule,
                    );
                    duration += start.elapsed();

                    nb_parallel += (schedule.nb_assigned_tx + schedule.read_only.len()) as f64;
                    // println!("Iter {}: {} (+ {})", iters, nb_parallel, schedule.nb_assigned_tx + schedule.read_only.len());
                    // println!("\tAssigned: Scheduled: {} txs, read_only: {} txs, postponed: {} txs",
                    //          schedule.nb_assigned_tx, schedule.read_only.len(), schedule.postponed.len() + schedule.exclusive.len());

                    execute_iter(
                        schedule.read_only.iter(),
                        &schedule.transactions,
                        &mut new_txs,
                        storage
                    );
                    for assigned in schedule.assignments.iter() {
                        execute_iter(
                            assigned.iter(),
                            &schedule.transactions,
                            &mut new_txs,
                            storage
                        );
                    }

                    // Add postponed back into chunk
                    for tx_index in schedule.postponed.iter() {
                        let tx = schedule.transactions[*tx_index].clone();
                        new_txs.push(tx);
                    }

                    schedule.clear();

                    mem::swap(&mut schedule.transactions, &mut new_txs);
                    new_txs.truncate(0);

                    if current_iter >= max {
                        break;
                    }
                    current_iter += 1;
                }
            }
            measurements[i] = duration;
        }

        let mean_latency = mean_ci(&measurements).0;
        let parallelism = nb_parallel/(total_size as f64);
        results.push((parallelism, mean_latency.as_micros(), total_size, current_iter));

        println!("{{\
        workload: '{workload:}', \
        scheduling: 'advanced({nb_executors:})', \
        size: {chunk_size:}, \
        parallelism: {parallelism:.3}, \
        latency: {:.3}, \
        iter: {current_iter:} \
        }},",
                 mean_latency.as_micros()
        );
    }

    // println!("Advanced scheduling (for {} executors): {:.3?}", nb_executors, results);
}

fn execute_iter<const A: usize, const P: usize>(
    iterator: Iter<usize>,
    txs: &Vec<Transaction<A, P>>,
    new_txs: &mut Vec<Transaction<A, P>>,
    storage: SharedStorage
) {
    for tx_index in iterator {
        let tx = txs[*tx_index].clone();
        unsafe {
            match tx.function.execute(tx, storage) {
                Another(generated) => {
                    new_txs.push(generated);
                },
                _ => {}
            }
        }
    }
}

// -------------------------------------------------------------------------------------------------
// TODO compute average nb_scheduled, nb_read_only, nb_postponed
fn micro_scheduling_batch<const A: usize, const P: usize>(
    batch: &Vec<Transaction<A, P>>,
    fractions: &Vec<usize>,
    nb_executors: &Vec<usize>,
    nb_repetitions: usize,
    measurements: &mut Vec<Duration>
)
{
    let batch_size = batch.len();
    let mut basic_scheduling = Vec::with_capacity(fractions.len()); // ------------
    let mut postponed = Vec::with_capacity(batch_size);
    let mut read_only = Vec::with_capacity(batch_size);
    for fraction in fractions.iter() {
        let chunk_size = batch_size/fraction;
        for i in 0..nb_repetitions {
            let chunk = Vec::from_iter(batch.iter().take(chunk_size).cloned());
            chunk.len();
            let start = Instant::now();
            // let (s, p) = Scheduling::schedule_chunk_old::<A, P>(chunk, scheduled, postponed, working_set);
            ReadOnlyScheduling::schedule::<A, P>(&chunk, &mut read_only, &mut postponed);
            measurements[i] = start.elapsed();

            // println!("\tBasic: Scheduled: {} txs, read_only: {} txs, postponed: {} txs", scheduled.len(), read_only.len(), postponed.len());
            postponed.truncate(0);
            read_only.truncate(0);
        }
        basic_scheduling.push(mean_ci(&measurements));
    }
    println!("Read-only scheduling: {:?}", basic_scheduling);
    println!();

    let mut medium_scheduling = Vec::with_capacity(fractions.len()); // ------------
    let mut scheduled = Vec::with_capacity(batch_size);
    let mut postponed = Vec::with_capacity(batch_size);
    let mut read_only = Vec::with_capacity(batch_size);
    for fraction in fractions.iter() {
        let chunk_size = batch_size/fraction;
        let mut working_set = AddressSet::with_capacity(2 * A * chunk_size);
        for i in 0..nb_repetitions {
            let chunk = Vec::from_iter(batch.iter().take(chunk_size).cloned());
            chunk.len();
            let start = Instant::now();
            // let (s, p) = Scheduling::schedule_chunk_old::<A, P>(chunk, scheduled, postponed, working_set);
            AddressScheduling::schedule::<A, P>(&chunk, &mut scheduled, &mut read_only, &mut postponed, &mut working_set);
            measurements[i] = start.elapsed();

            // println!("\tMedium: Scheduled: {} txs, read_only: {} txs, postponed: {} txs", scheduled.len(), read_only.len(), postponed.len());
            scheduled.truncate(0);
            postponed.truncate(0);
            read_only.truncate(0);
            working_set.clear();
        }
        medium_scheduling.push(mean_ci(&measurements));
    }
    println!("Address scheduling: {:?}", medium_scheduling);
    println!();

    let mut rw_scheduling = Vec::with_capacity(fractions.len()); // ---------------
    let mut scheduled = Vec::with_capacity(batch_size);
    let mut postponed = Vec::with_capacity(batch_size);
    let mut read_only = Vec::with_capacity(batch_size);
    for fraction in fractions.iter() {
        let chunk_size = batch_size/fraction;
        let mut address_map = HashMap::with_capacity_and_hasher(2 * A * chunk_size, BuildHasherDefault::default());
        for i in 0..nb_repetitions {
            let transactions = Arc::new(Vec::from_iter(batch.iter().take(chunk_size).cloned()));
            let mut chunk = (0..chunk_size).collect_vec();
            let start = Instant::now();
            BasicScheduling::schedule_chunk_new::<A, P>(
                &transactions,
                &mut chunk,
                &mut scheduled,
                &mut postponed,
                &mut address_map,
                &mut read_only
            );
            measurements[i] = start.elapsed();
            // println!("\tR/W: Scheduled: {} txs, read_only: {} txs, postponed: {} txs", scheduled.len(), read_only.len(), postponed.len());

            scheduled.truncate(0);
            postponed.truncate(0);
            read_only.truncate(0);
            address_map.clear();
        }
        rw_scheduling.push(mean_ci(&measurements));
    }
    println!("Basic scheduling: {:?}", rw_scheduling);
    println!();

    let mut assign_scheduling = Vec::with_capacity(fractions.len());
    let mut new_reads = Vec::with_capacity(A * batch_size);
    let mut new_writes = Vec::with_capacity(A * batch_size);
    for nb_executor in nb_executors {
        let mut schedule = Schedule::new(batch_size, *nb_executor);
        for fraction in fractions.iter() {
            let chunk_size = batch_size/fraction;
            let mut address_map = HashMap::with_capacity_and_hasher(2 * A * chunk_size, BuildHasherDefault::default());
            for i in 0..nb_repetitions {
                schedule.transactions.extend(batch.iter().take(chunk_size).cloned());
                let start = Instant::now();
                Scheduling::schedule_chunk_assign::<A, P>(
                    &mut address_map,
                    &mut new_reads,
                    &mut new_writes,
                    &mut schedule,
                );
                measurements[i] = start.elapsed();
                // println!("\tAssigned: Scheduled: {} txs, read_only: {} txs, postponed: {} txs",
                //          schedule.nb_assigned_tx, schedule.read_only.len(), schedule.postponed.len() + schedule.exclusive.len());
                new_reads.truncate(0);
                new_writes.truncate(0);
                address_map.clear();
                schedule.clear();
            }
            assign_scheduling.push(mean_ci(&measurements));
        }
        println!("Advanced scheduling ({} executors): {:?}", nb_executor, assign_scheduling);
        // println!();
        assign_scheduling.truncate(0);
    }

    println!();
}

fn micro_scheduling_quality<const A: usize, const P: usize>(
    batch: &Vec<Transaction<A, P>>,
    fractions: &Vec<usize>,
    nb_executors: &Vec<usize>,
)
{
    let batch_size = batch.len();
    let mut basic_scheduling = Vec::with_capacity(fractions.len()); // ------------
    let mut postponed = Vec::with_capacity(batch_size);
    let mut read_only = Vec::with_capacity(batch_size);
    for fraction in fractions.iter() {
        let chunk_size = batch_size/fraction;
        let chunk = Vec::from_iter(batch.iter().take(chunk_size).cloned());
        chunk.len();
        ReadOnlyScheduling::schedule::<A, P>(&chunk, &mut read_only, &mut postponed);
        // println!("\tBasic: Scheduled: {} txs, read_only: {} txs, postponed: {} txs", scheduled.len(), read_only.len(), postponed.len());

        let nb_parallel = read_only.len() as f64;
        basic_scheduling.push(nb_parallel/(chunk_size as f64));
        postponed.truncate(0);
        read_only.truncate(0);
    }
    println!("Basic scheduling: {:.2?}", basic_scheduling);
    println!();

    let mut medium_scheduling = Vec::with_capacity(fractions.len()); // ------------
    let mut scheduled = Vec::with_capacity(batch_size);
    let mut postponed = Vec::with_capacity(batch_size);
    let mut read_only = Vec::with_capacity(batch_size);
    for fraction in fractions.iter() {
        let chunk_size = batch_size/fraction;
        let mut working_set = AddressSet::with_capacity(2 * A * chunk_size);
        let chunk = Vec::from_iter(batch.iter().take(chunk_size).cloned());
        AddressScheduling::schedule::<A, P>(&chunk, &mut scheduled, &mut read_only, &mut postponed, &mut working_set);
        // println!("\tMedium: Scheduled: {} txs, read_only: {} txs, postponed: {} txs", scheduled.len(), read_only.len(), postponed.len());
        let nb_parallel = (scheduled.len() + read_only.len()) as f64;
        medium_scheduling.push(nb_parallel/(chunk_size as f64));
        scheduled.truncate(0);
        postponed.truncate(0);
        read_only.truncate(0);
        working_set.clear();
    }
    println!("Medium scheduling: {:.2?}", medium_scheduling);
    println!();

    let mut rw_scheduling = Vec::with_capacity(fractions.len()); // ---------------
    let mut scheduled = Vec::with_capacity(batch_size);
    let mut postponed = Vec::with_capacity(batch_size);
    let mut read_only = Vec::with_capacity(batch_size);
    for fraction in fractions.iter() {
        let chunk_size = batch_size/fraction;
        let mut address_map = HashMap::with_capacity_and_hasher(2 * A * chunk_size, BuildHasherDefault::default());
        let transactions = Arc::new(Vec::from_iter(batch.iter().take(chunk_size).cloned()));
        let mut chunk = (0..chunk_size).collect_vec();
        BasicScheduling::schedule_chunk_new::<A, P>(
            &transactions,
            &mut chunk,
            &mut scheduled,
            &mut postponed,
            &mut address_map,
            &mut read_only
        );
        let nb_parallel = (scheduled.len() + read_only.len()) as f64;
        rw_scheduling.push(nb_parallel/(chunk_size as f64));
        scheduled.truncate(0);
        postponed.truncate(0);
        read_only.truncate(0);
        address_map.clear();

    }
    println!("Read/Write scheduling: {:.2?}", rw_scheduling);
    println!();

    let mut assign_scheduling = Vec::with_capacity(fractions.len());
    let mut new_reads = Vec::with_capacity(A * batch_size);
    let mut new_writes = Vec::with_capacity(A * batch_size);
    for nb_executor in nb_executors {
        let mut schedule = Schedule::new(batch_size, *nb_executor);
        for fraction in fractions.iter() {
            let chunk_size = batch_size/fraction;
            let mut address_map = HashMap::with_capacity_and_hasher(2 * A * chunk_size, BuildHasherDefault::default());
            schedule.transactions.extend(batch.iter().take(chunk_size).cloned());
            Scheduling::schedule_chunk_assign::<A, P>(
                &mut address_map,
                &mut new_reads,
                &mut new_writes,
                &mut schedule,
            );
            let nb_parallel = (schedule.nb_assigned_tx + schedule.read_only.len()) as f64;
            assign_scheduling.push(nb_parallel/(chunk_size as f64));
            schedule.clear();
            new_reads.truncate(0);
            new_writes.truncate(0);
            address_map.clear();
        }
        println!("Assignment scheduling ({} executors): {:.2?}", nb_executor, assign_scheduling);
        assign_scheduling.truncate(0);
    }

    println!();
}

struct ReadOnlyScheduling;
impl ReadOnlyScheduling {
    pub fn schedule<const A: usize, const P: usize>(
        chunk: &Vec<Transaction<A, P>>,
        read_only: &mut Vec<usize>,
        postponed: &mut Vec<usize>,
        // a: Instant,
    ) {
        for (tx_index, tx) in chunk.iter().enumerate() {
            match tx.tpe() {
                TransactionType::ReadOnly => {
                    read_only.push(tx_index);
                    continue;
                },
                _ => {
                    postponed.push(tx_index);
                    continue;
                },
            }
        }
    }
}

struct AddressScheduling;
impl AddressScheduling {
    pub fn schedule<const A: usize, const P: usize>(
        chunk: &Vec<Transaction<A, P>>,
        scheduled: &mut Vec<usize>,
        read_only: &mut Vec<usize>,
        postponed: &mut Vec<usize>,
        working_set: &mut AddressSet,
        // a: Instant,
    ) {
        'backlog: for (tx_index, tx) in chunk.iter().enumerate() {
            match tx.tpe() {
                TransactionType::ReadOnly => {
                    read_only.push(tx_index);
                    continue;
                },
                TransactionType:: Exclusive => {
                    postponed.push(tx_index);
                    continue;
                },
                _ => {

                }
            }

            let (possible_reads, possible_writes) = tx.accesses();
            let reads = possible_reads.as_ref().map_or([].as_slice(), |inside| inside.as_slice());
            let writes = possible_writes.as_ref().map_or([].as_slice(), |inside| inside.as_slice());
            'accesses: for access in reads.iter().chain(writes.iter()) {
                match access {
                    AccessPattern::Address(addr) => {
                        if !working_set.insert(*addr) {
                            // Can't add tx to schedule
                            postponed.push(tx_index);
                            continue 'backlog;
                        }
                    },
                    AccessPattern::Range(from, to) => {
                        assert!(from < to);
                        'range: for addr in (*from)..(*to) {
                            if !working_set.insert(addr) {
                                // Can't add tx to schedule
                                postponed.push(tx_index);
                                continue 'backlog;
                            }
                        }
                    },
                    AccessPattern::All => {
                        postponed.push(tx_index);
                        continue 'backlog;
                    },
                    AccessPattern::Done => {
                        // break 'accesses;
                    }
                }
            }

            scheduled.push(tx_index)
        }
    }
}

type Map = HashMap<StaticAddress, AccessType, BuildHasherDefault<AHasher>>;
struct BasicScheduling;
impl BasicScheduling {
    pub fn schedule_chunk_new<const A: usize, const P: usize>(
        transactions: &Arc<Vec<Transaction<A, P>>>,
        mut chunk: &mut Vec<usize>,
        mut scheduled: &mut Vec<usize>,
        mut postponed: &mut Vec<usize>,
        mut address_map: &mut Map,
        read_only: &mut Vec<usize>
    )
    {

        let mut remainder = 0;

        let can_read = |addr: StaticAddress, map: &mut Map| {
            let entry = map.entry(addr).or_insert(AccessType::Read);
            *entry == AccessType::Read
        };
        let can_write = |addr: StaticAddress, map: &mut Map| {
            let entry = map.entry(addr).or_insert(AccessType::TryWrite);
            if *entry == AccessType::TryWrite {
                *entry = AccessType::Write;
                true
            } else {
                false
            }
        };

        // let init_duration = a.elapsed();
        // let mut base_case_duration = Duration::from_secs(0);
        // let mut reads_duration = Duration::from_secs(0);
        // let mut writes_duration = Duration::from_secs(0);

        'backlog: for tx_index in chunk.drain(0..) {
            let tx = transactions.get(tx_index).unwrap();

            match tx.tpe() {
                TransactionType::ReadOnly => {
                    read_only.push(tx_index);
                    continue;
                },
                TransactionType:: Exclusive => {
                    postponed.push(tx_index);
                    continue;
                },
                TransactionType::Writes => {
                }
            }

            remainder += 1;
            // let base_case_start = Instant::now();
            let (possible_reads, possible_writes) = tx.accesses();
            let reads = possible_reads.as_ref().map_or([].as_slice(), |inside| inside.as_slice());
            let writes = possible_writes.as_ref().map_or([].as_slice(), |inside| inside.as_slice());

            // Tx without any memory accesses ------------------------------------------------------
            if writes.is_empty() {
                read_only.push(tx_index);
                continue 'backlog;
            }
            // base_case_duration += base_case_start.elapsed();

            // NB: A tx might be postponed after having some of its addresses added to the address set
            // It is probably too expensive to rollback those changes. TODO Check
            // Start with reads because they are less problematic if the tx is postponed
            // Process reads -----------------------------------------------------------------------
            // let read_start = Instant::now();
            'reads: for read in reads {
                match read {
                    AccessPattern::Address(addr) => {
                        if !can_read(*addr, &mut address_map) {
                            postponed.push(tx_index);
                            // reads_duration += read_start.elapsed();
                            continue 'backlog;
                        }
                    },
                    AccessPattern::Range(from, to) => {
                        assert!(from < to);
                        'range: for addr in (*from)..(*to) {
                            if !can_read(addr, &mut address_map) {
                                postponed.push(tx_index);
                                // reads_duration += read_start.elapsed();
                                continue 'backlog;
                            }
                        }
                    },
                    AccessPattern::All => {
                        postponed.push(tx_index);
                        continue 'backlog;
                    },
                    AccessPattern::Done => {
                        break 'reads;
                    }
                }
            }
            // reads_duration += read_start.elapsed();

            // Process writes ----------------------------------------------------------------------
            // let writes_start = Instant::now();
            'writes: for write in writes {
                match write {
                    AccessPattern::Address(addr) => {
                        if !can_write(*addr, &mut address_map) {
                            postponed.push(tx_index);
                            // writes_duration += writes_start.elapsed();
                            continue 'backlog;
                        }
                    },
                    AccessPattern::Range(from, to) => {
                        assert!(from < to);
                        'range: for addr in (*from)..(*to) {
                            if !can_write(addr, &mut address_map) {
                                postponed.push(tx_index);
                                // writes_duration += writes_start.elapsed();
                                continue 'backlog;
                            }
                        }
                    },
                    AccessPattern::All => {
                        postponed.push(tx_index);
                        continue 'backlog;
                    },
                    AccessPattern::Done => {
                        // writes_duration += writes_start.elapsed();
                        break 'writes;
                    }
                }
            }
            // writes_duration += writes_start.elapsed();

            // Transaction can be executed in parallel
            scheduled.push(tx_index);
        }

        // let total = a.elapsed();
        // println!("Scheduled {:?}", scheduled);
        // println!("Scheduled: {}, postponed: {}, took {:?}", scheduled.len(), postponed.len(), total);
        // total, (init, base_case, reads, writes)
        // println!("\ttook {:.3?} \t({:?} µs, {:?} µs, {:?} µs, {:?} µs)", total, init_duration.as_micros(), base_case_duration.as_micros(), reads_duration.as_micros(), writes_duration.as_micros());
        // (scheduled, postponed)
    }
}

fn dhashmap_batch(batch_size: usize, storage_size: usize, rng: &mut StdRng, weights: [f64; 4]) -> (Vec<Transaction<5, ENTRY_SIZE>>, VmStorage) {
    // let weights = [0.2, 0.2, 0.2, 0.2, ];
    let addresses = [0, 0, 0, 0, 0];
    let operations = [
        PieceDHashMap(PiecedOperation::GetComputeAndFind),
        PieceDHashMap(PiecedOperation::InsertComputeAndFind),
        PieceDHashMap(PiecedOperation::RemoveComputeAndFind),
        PieceDHashMap(PiecedOperation::HasComputeAndFind),
    ];
    // let nb_buckets = 64;
    // let bucket_capacity_elems = 64;
    let max_nb_buckets = (NB_BUCKETS << 4) as usize;
    let max_nb_keys = max_nb_buckets * BUCKET_CAPACITY_ELEMS as usize;
    let nb_keys = (2 * max_nb_keys) / 3;
    let key_space = (0..nb_keys).map(|key| key as FunctionParameter).collect_vec();

    let map_size = |nb_buckets: usize, bucket_capacity_elems: usize, entry_size: usize| {
        (2 + nb_buckets + nb_buckets * (1 + (bucket_capacity_elems as usize) * entry_size))
    };

    let mut storage = VmStorage::new(max(
        map_size(NB_BUCKETS, BUCKET_CAPACITY_ELEMS, ENTRY_SIZE),
        storage_size
    ));
    DHashMap::init::<ENTRY_SIZE>(&mut storage.content, NB_BUCKETS, BUCKET_CAPACITY_ELEMS);

    let dist2 = WeightedIndex::new(weights).unwrap();
    let mut tx_params = [0 as FunctionParameter; ENTRY_SIZE];
    let batch = (0..batch_size).map(|tx_index| {
        let op = operations[dist2.sample(rng)];
        let key = *key_space.choose(rng).unwrap_or(&0);

        for i in 0..ENTRY_SIZE { tx_params[i] = (ENTRY_SIZE - i) as FunctionParameter; }
        tx_params[0] = key;

        let tx = Transaction {
            sender: tx_index as SenderAddress,
            function: op,
            tx_index,
            addresses,
            params: tx_params,
        };

        // We are not interested in the scheduling latency of transactions without any accesses
        // => Skip the first piece
        unsafe {
            match tx.function.execute(tx, storage.get_shared()) {
                Another(generated) => generated,
                _ => panic!()
            }
        }

    }).collect_vec();

    (batch, storage)
}
//endregion

//region Benchmarking HashMaps ---------------------------------------------------------------------
pub fn bench_hashmaps(nb_iter: usize, addr_per_tx: usize, batch_size: usize) {
    println!("Batch of {} tx with {} addr per tx = {} insertions ({} reps)", batch_size, addr_per_tx, batch_size * addr_per_tx, nb_iter);
    let mut rng = StdRng::seed_from_u64(10);
    let storage_size = 100 * batch_size;
    let mut addresses: Vec<StaticAddress> = (0..storage_size).map(|el| el as StaticAddress).collect();
    addresses.shuffle(&mut rng);
    addresses.truncate(addr_per_tx * batch_size);
    let map_capacity = 2 * addresses.len();

    let mut measurements = vec!();

    measurements.push(HashMap::<StaticAddress, StaticAddress>::measure(nb_iter, &addresses));
    measurements.push(HashMapNoHash::measure(nb_iter, &addresses));
    measurements.push(HashMapAHash::measure(nb_iter, &addresses));

    measurements.push(BrownMap::<StaticAddress, StaticAddress>::measure(nb_iter, &addresses));
    measurements.push(BrownMapNoHash::measure(nb_iter, &addresses));

    measurements.push(ThinMap::<StaticAddress, StaticAddress>::measure(nb_iter, &addresses));
    measurements.push(ThinMapNoHash::measure(nb_iter, &addresses));
    measurements.push(ThinMapAHash::measure(nb_iter, &addresses));

    println!("{}", Table::new(measurements).to_string());
    println!();

    //Expected results: https://github.com/rust-lang/hashbrown
}

pub fn bench_hashmaps_clear(nb_iter: usize, addr_per_tx: usize, batch_size: usize) {
    println!("Batch of {} tx with {} addr per tx = {} insertions ({} reps)", batch_size, addr_per_tx, batch_size * addr_per_tx, nb_iter);
    let mut rng = StdRng::seed_from_u64(10);
    let storage_size = 100 * batch_size;
    let mut addresses: Vec<StaticAddress> = (0..storage_size).map(|el| el as StaticAddress).collect();
    addresses.shuffle(&mut rng);
    addresses.truncate(addr_per_tx * batch_size);
    let map_capacity = 2 * addresses.len();

    let mut measurements = vec!();

    measurements.push(HashMap::<StaticAddress, StaticAddress>::measure_clear(nb_iter, &addresses));
    measurements.push(HashMapNoHash::measure_clear(nb_iter, &addresses));
    measurements.push(HashMapAHash::measure_clear(nb_iter, &addresses));

    measurements.push(BrownMap::<StaticAddress, StaticAddress>::measure_clear(nb_iter, &addresses));
    measurements.push(BrownMapNoHash::measure_clear(nb_iter, &addresses));

    measurements.push(ThinMap::<StaticAddress, StaticAddress>::measure_clear(nb_iter, &addresses));
    measurements.push(ThinMapNoHash::measure_clear(nb_iter, &addresses));
    measurements.push(ThinMapAHash::measure_clear(nb_iter, &addresses));

    println!("{}", Table::new(measurements).to_string());
    println!();

    //Expected results: https://github.com/rust-lang/hashbrown
}

trait HashMapWrapper {
    fn print_name() -> String where Self: Sized;
    fn with_capacity(capacity: usize) -> Self where Self: Sized;
    fn insert(&mut self, key: StaticAddress, value: StaticAddress) -> Option<StaticAddress>;
    fn clear(&mut self);
    fn measure(nb_iter: usize, to_insert: &Vec<StaticAddress>) -> HashMapMeasurement where Self: Sized {

        let mut durations = Vec::with_capacity(nb_iter);
        let capacity = 2 * to_insert.len(); // capacity must be slightly larger than the number of value to insert

        for _ in 0..nb_iter {
            let mut map: Box<dyn HashMapWrapper> = Box::new(Self::with_capacity(capacity));

            let start = Instant::now();
            for addr in to_insert.iter() {
                map.insert(*addr, *addr);
            }
            durations.push(start.elapsed());
        }
        let (mean, ci) = mean_ci(&durations);

        HashMapMeasurement {
            hash_map_type: Self::print_name(),
            mean_latency: format!("{:?}", mean),
            ci: format!("{:?}", ci),
        }
    }

    fn measure_clear(nb_iter: usize, to_insert: &Vec<StaticAddress>) -> HashMapMeasurement where Self: Sized {

        let mut durations = Vec::with_capacity(nb_iter);
        let capacity = 2 * to_insert.len(); // capacity must be slightly larger than the number of value to insert

        for _ in 0..nb_iter {
            let mut map: Box<dyn HashMapWrapper> = Box::new(Self::with_capacity(capacity));

            for addr in to_insert.iter() {
                map.insert(*addr, *addr);
            }
            let start = Instant::now();
            map.clear();
            durations.push(start.elapsed());
        }
        let (mean, ci) = mean_ci(&durations);

        HashMapMeasurement {
            hash_map_type: Self::print_name(),
            mean_latency: format!("{:?}", mean),
            ci: format!("{:?}", ci),
        }
    }
}

#[derive(Tabled)]
struct HashMapMeasurement {
    pub hash_map_type: String,
    pub mean_latency: String,
    pub ci: String,
}

//region Hash map wrappers
// HashMap
impl HashMapWrapper for HashMap<StaticAddress, StaticAddress> {
    fn print_name() -> String where Self: Sized {
        format!("HashMap")
    }

    fn with_capacity(capacity: usize) -> Self where Self: Sized {
        HashMap::with_capacity(capacity)
    }

    fn insert(&mut self, key: StaticAddress, value: StaticAddress) -> Option<StaticAddress> {
        self.insert(key, value)
    }

    fn clear(&mut self) {
        self.clear();
    }
}

type HashMapNoHash = HashMap<StaticAddress, StaticAddress, BuildHasherDefault<NoHashHasher<StaticAddress>>>;
impl HashMapWrapper for HashMapNoHash {
    fn print_name() -> String where Self: Sized {
        format!("HashMap (NoHash)")
    }
    fn with_capacity(capacity: usize) -> Self where Self: Sized {
        HashMap::with_capacity_and_hasher(capacity, BuildNoHashHasher::default())
    }

    fn insert(&mut self, key: StaticAddress, value: StaticAddress) -> Option<StaticAddress> {
        self.insert(key, value)
    }

    fn clear(&mut self) {
        self.clear();
    }
}

type HashMapAHash = HashMap<StaticAddress, StaticAddress, BuildHasherDefault<AHasher>>;
impl HashMapWrapper for HashMapAHash {
    fn print_name() -> String where Self: Sized {
        format!("HashMap (AHash)")
    }
    fn with_capacity(capacity: usize) -> Self where Self: Sized {
        HashMap::with_capacity_and_hasher(capacity, BuildHasherDefault::default())
    }

    fn insert(&mut self, key: StaticAddress, value: StaticAddress) -> Option<StaticAddress> {
        self.insert(key, value)
    }

    fn clear(&mut self) {
        self.clear();
    }
}

// BrownMap
impl HashMapWrapper for BrownMap<StaticAddress, StaticAddress> {
    fn print_name() -> String where Self: Sized {
        format!("BrownMap")
    }

    fn with_capacity(capacity: usize) -> Self where Self: Sized {
        BrownMap::with_capacity(capacity)
    }

    fn insert(&mut self, key: StaticAddress, value: StaticAddress) -> Option<StaticAddress> {
        self.insert(key, value)
    }

    fn clear(&mut self) {
        self.clear();
    }
}

type BrownMapNoHash = BrownMap<StaticAddress, StaticAddress, BuildHasherDefault<NoHashHasher<StaticAddress>>>;
impl HashMapWrapper for BrownMapNoHash {
    fn print_name() -> String where Self: Sized {
        format!("BrownMap (NoHash)")
    }

    fn with_capacity(capacity: usize) -> Self where Self: Sized {
        BrownMap::with_capacity_and_hasher(capacity, BuildNoHashHasher::default())
    }

    fn insert(&mut self, key: StaticAddress, value: StaticAddress) -> Option<StaticAddress> {
        self.insert(key, value)
    }

    fn clear(&mut self) {
        self.clear();
    }
}

// ThinMap
impl HashMapWrapper for ThinMap<StaticAddress, StaticAddress> {
    fn print_name() -> String where Self: Sized {
        format!("ThinMap")
    }

    fn with_capacity(capacity: usize) -> Self where Self: Sized {
        ThinMap::with_capacity(capacity)
    }

    fn insert(&mut self, key: StaticAddress, value: StaticAddress) -> Option<StaticAddress> {
        self.insert(key, value)
    }

    fn clear(&mut self) {
        self.clear();
    }
}

type ThinMapNoHash = ThinMap<StaticAddress, StaticAddress, BuildHasherDefault<NoHashHasher<StaticAddress>>>;
impl HashMapWrapper for ThinMapNoHash {
    fn print_name() -> String where Self: Sized {
        format!("ThinMap (NoHash)")
    }

    fn with_capacity(capacity: usize) -> Self where Self: Sized {
        ThinMap::with_capacity_and_hasher(capacity, BuildNoHashHasher::default())
    }

    fn insert(&mut self, key: StaticAddress, value: StaticAddress) -> Option<StaticAddress> {
        self.insert(key, value)
    }

    fn clear(&mut self) {
        self.clear();
    }
}

type ThinMapAHash = ThinMap<StaticAddress, StaticAddress, BuildHasherDefault<AHasher>>;
impl HashMapWrapper for ThinMapAHash {
    fn print_name() -> String where Self: Sized {
        format!("ThinMap (AHash)")
    }

    fn with_capacity(capacity: usize) -> Self where Self: Sized {
        ThinMap::with_capacity_and_hasher(capacity, BuildHasherDefault::default())
    }

    fn insert(&mut self, key: StaticAddress, value: StaticAddress) -> Option<StaticAddress> {
        self.insert(key, value)
    }

    fn clear(&mut self) {
        self.clear();
    }
}
//endregion

//endregion

//region NUMA latency ------------------------------------------------------------------------------
const KILO_OFFSET: usize = 10;
const MEGA_OFFSET: usize = 20;
const GIGA_OFFSET: usize = 30;
const KB: usize = 1 << KILO_OFFSET;
const MB: usize = 1 << MEGA_OFFSET;
const GB: usize = 1 << GIGA_OFFSET;

const SEED: u64 = 10;
const MILLION: usize = 1 * 1000 * 1000; // 1M operations per iteration

// getconf -a | grep CACHE
// Laptop
const L1_SIZE: usize = 32 * KB;    // per core
const L2_SIZE: usize = 256 * KB;   // per core
const L3_SIZE: usize = 6 * MB;     // shared
const CACHE_LINE_SIZE: u8 = 64;

// // AWS
// let L1_SIZE = 48 * kilo;            // 48 KB (per core)
// let L2_SIZE = 1 * mega + mega/4;    // 1.25 MB (per core)
// let L3_SIZE = 54 * mega;            // 54 MB (shared)
// const CACHE_LINE_SIZE: usize = 64;

pub fn adapt_unit(size_b: usize) -> String {
    match size_b {
        s if s < KB => format!("{}B", s),
        s if s < MB => format!("{}KB", s >> KILO_OFFSET),
        s if s < GB => format!("{}MB", s >> MEGA_OFFSET),
        s => format!("{}GB", s >> GIGA_OFFSET),
    }
}

pub async fn random_data_init(size: usize, memory_core: usize) -> anyhow::Result<Vec<u8>> {
    let random_array = tokio::spawn(async move {

        // print!("Pinning memory to core {}...", memory_core);
        let pinned = core_affinity::set_for_current(CoreId{ id: memory_core });
        if !pinned {
            return Err(anyhow!("Unable to pin to memory core to CPU #{}", memory_core));
        }

        let mut rng = StdRng::seed_from_u64(SEED);
        let mut arr = vec![0; size];
        for i in 0..size {
            arr[i] = (rng.next_u32() & 0b11111111) as u8;
        }

        Ok(arr)
    }).await?;

    random_array
}

fn random_step_size_k(size: usize, arr: &mut Vec<u8>, nb_operations: usize) -> usize {
    let mut idx: usize = 0;

    for _ in 0..nb_operations {
        // Accesses a random cache line
        let pos = idx & (size - 1);
        arr[pos] += 1;
        idx += (arr[pos] * CACHE_LINE_SIZE) as usize;
    }

    return idx;
}

pub async fn numa_latency(
    execution_core: usize,
    memory_core: usize,
    array_size_bytes: usize,
    nb_operations: usize,
    nb_iterations: usize
) -> anyhow::Result<Measurement> {
    let target_duration = Duration::from_millis(500);
    let start = Instant::now();

    let pinned = core_affinity::set_for_current(CoreId{ id: execution_core });
    if !pinned {
        return Err(anyhow!("Unable to pin to execution core to CPU #{}", execution_core));
    }

    let mut random_array = random_data_init(array_size_bytes, memory_core).await?;

    let mut latencies = Vec::with_capacity(nb_iterations);
    // let mut results = Vec::with_capacity(nb_iterations);

    for i in 0..nb_iterations {
        let start_iteration = Instant::now();
        let _res = random_step_size_k(array_size_bytes, &mut random_array, nb_operations);
        latencies.push(start_iteration.elapsed());
        // results.push(_res);

        if start.elapsed() > target_duration && i > 100 {
            break;
        }
    }

    let total_duration = start.elapsed();

    Ok(Measurement::new(
        execution_core,
        memory_core,
        array_size_bytes,
        &latencies,
        total_duration,
        nb_operations))
}

pub async fn all_numa_latencies(nb_cores: usize, from_power: usize, to_power: usize) -> anyhow::Result<()> {

    let nb_sizes = to_power - from_power;
    let sizes_bytes = (from_power..to_power)
        .map(|power_of_two| ((1 << power_of_two) << KILO_OFFSET) as usize)
        .collect_vec();

    let mut measurements = Vec::with_capacity(nb_cores * nb_sizes);

    let mut core_measurements = vec![vec!(); nb_cores];
    let mut sizes_measurements = vec![vec!(); nb_sizes];

    for core in 0..nb_cores {
        let from_size = adapt_unit((1 << from_power) << KILO_OFFSET);
        let to_size = adapt_unit((1 << to_power) << KILO_OFFSET);
        eprint!("Benchmarking latency to core {} from {} to {}", core, from_size, to_size);

        for (size_index, array_size_bytes) in sizes_bytes.iter().enumerate() {
            // eprintln!("\n\tarray size = {}", adapt_unit(array_size_bytes));
            let measurement = numa_latency(0, core,
                                           *array_size_bytes, MILLION, 300).await?;

            core_measurements[core].push(SizeLatencyMeasurement::from(&measurement));
            sizes_measurements[size_index].push(CoreLatencyMeasurement::from(&measurement));
            measurements.push(measurement);
            eprint!(".")
        }
        println!();
    }

    println!("Latency per size: ====================================================================");
    for (core, measurements) in core_measurements.iter().enumerate() {
        println!("Core#{}:", core);
        println!("{}", Table::new(measurements.clone()).to_string());
        println!()
    }

    // println!("Latency per core : ===================================================================");
    // for (measurements, power_of_two) in sizes_measurements.iter().zip(from_power..to_power) {
    //     let array_size_bytes = (1 << power_of_two) << KILO_OFFSET;
    //     println!("Array size: {}", adapt_unit(array_size_bytes));
    //     println!("{}", Table::new(measurements.clone()).to_string());
    //     println!()
    // }

    // println!("All measurement: =====================================================================");
    // for measurement in measurements {
    //     println!("{}", measurement.to_json());
    // }
    /*
    let sizes_str = ['4KB', '8KB', '16KB', ...];
    let sizes = [4000, 8000, 16000, ...];
    let all_core_values = [
        ['Core#0', 2.282, 2.215, 2.198],
        ['Core#1', 2.218, 2.205, 2.200],
        ['Core#2', 2.195, 2.201, 2.194],
        ['Core#3', 2.229, 2.179, 2.200],
        ...
      ];
      // getconf -a | grep CACHE
     */
    let sizes_str = sizes_bytes.iter().map(|size| adapt_unit(*size)).collect_vec();
    println!("let sizes_str = {:?};", sizes_str);
    println!("let sizes = {:?};", sizes_bytes);
    println!("let all_core_values = [");
    for (core, measurements) in core_measurements.into_iter().enumerate() {
        print!("\t['Core#{}'", core);
        for measurement in measurements.into_iter() {
            let mut str = measurement.mean;
            str.retain(|c| !("nµms".contains(c)));
            print!(", {}", str);
        }
        println!("],");
    }
    println!("];");


    Ok(())
}
//endregion

//endregion

//region Transaction sizes -------------------------------------------------------------------------
pub fn transaction_sizes(nb_schedulers: usize, nb_workers: usize) {

    fn sizes<const NB_ADDRESSES: usize, const NB_PARAMS: usize>(nb_schedulers: usize, nb_workers: usize) {
        let tx_size_bytes = mem::size_of::<Transaction<NB_ADDRESSES, NB_PARAMS>>();
        let batch_size = 65536;
        let batch_size_bytes = batch_size * tx_size_bytes;
        let chunk_size_bytes = batch_size_bytes / nb_schedulers;
        let executor_share_bytes = chunk_size_bytes / nb_workers;
        println!("Size tx<{}, {}> ~ {} B \n\tbatch: {}, chunk: {}, worker_share: {}",
                 NB_ADDRESSES, NB_PARAMS, tx_size_bytes, adapt_unit(batch_size_bytes),
                 adapt_unit(chunk_size_bytes), adapt_unit(executor_share_bytes));
    }


    println!("{} schedulers, {} workers", nb_schedulers, nb_workers);
    sizes::<0, 1>(nb_schedulers, nb_workers);
    sizes::<2, 1>(nb_schedulers, nb_workers);
    sizes::<2, 10>(nb_schedulers, nb_workers);
    sizes::<2, 20>(nb_schedulers, nb_workers);
    sizes::<2, 26>(nb_schedulers, nb_workers);
    println!();

    // let tx_size_bytes = mem::size_of::<Arc<Transaction<2, 10>>>();
    // println!("Arc size = {} B", tx_size_bytes);


    // Need at least 8 schedulers so that a chunk can fit in L2 memory -> at most 24 executors
    // Worker share seems to always be able to fit in L1
    // => most of L2 cache of worker will serve to keep storage hot
}
//endregion

/*
pub async fn hwloc_test() {
    fn print_children(topo: &Topology, obj: &TopologyObject, depth: usize) {
        let mut padding = std::iter::repeat("  ").take(depth)
            .collect::<String>();
        println!("{}{}: #{}", padding, obj, obj.os_index());

        for i in 0..obj.arity() {
            print_children(topo, obj.children()[i as usize], depth + 1);
        }
    }

    fn last_core(topo: &mut Topology) -> &TopologyObject {
        let core_depth = topo.depth_or_below_for_type(&ObjectType::Core).unwrap();
        let all_cores = topo.objects_at_depth(core_depth);
        all_cores.last().unwrap()
    }

    let mut topo = Topology::new().expect("This lib is really badly documented...");

    // ---------------------------------------------------------------------------------------------
    println!("*** Printing objects at different levels tree");
    for i in 0..topo.depth() {
        println!("*** Level {}", i);

        for (idx, object) in topo.objects_at_depth(i).iter().enumerate() {
            println!("{}: {}", idx, object);
        }
    }
    println!();

    // ---------------------------------------------------------------------------------------------
    println!("*** Printing overall tree");
    print_children(&topo, topo.object_at_root(), 0);

    // ---------------------------------------------------------------------------------------------
    println!("*** Pinning current thread of current process to a core");
    // Grab last core and exctract its CpuSet
    let mut cpuset = last_core(&mut topo).cpuset().unwrap();

    // Get only one logical processor (in case the core is SMT/hyper-threaded).
    cpuset.singlify();

    println!("Before Bind: {:?}", topo.get_cpubind(CpuBindFlags::CPUBIND_THREAD));
    // Last CPU Location for this PID (not implemented on all systems)
    if let Some(l) = topo.get_cpu_location(CpuBindFlags::CPUBIND_THREAD) {
        println!("Last Known CPU Location: {:?}", l);
    }
    println!();

    let sleep_duration = 5;
    println!("Sleeping {}s to see if task changes core", sleep_duration);
    let _ = tokio::time::sleep(Duration::from_secs(sleep_duration)).await;
    println!();
    // let _ = std::thread::sleep(Duration::from_secs(sleep_duration));
    println!("Before Bind: {:?}", topo.get_cpubind(CpuBindFlags::CPUBIND_THREAD));
    // Last CPU Location for this PID (not implemented on all systems)
    if let Some(l) = topo.get_cpu_location(CpuBindFlags::CPUBIND_THREAD) {
        println!("Last Known CPU Location: {:?}", l);
    }

    // Bind to one core.
    // topo.set_cpubind(cpuset, CpuBindFlags::CPUBIND_THREAD);
    let pinned = core_affinity::set_for_current(CoreId{ id: 1 });
    if !pinned {
        panic!("Unable to pin to executor core to CPU #{}", 1);
    }

    println!("After Bind: {:?}", topo.get_cpubind(CpuBindFlags::CPUBIND_THREAD));

    // Last CPU Location for this PID (not implemented on all systems)
    if let Some(l) = topo.get_cpu_location(CpuBindFlags::CPUBIND_THREAD) {
        println!("Last Known CPU Location: {:?}", l);
    }
}
*/