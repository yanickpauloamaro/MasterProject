use std::fmt::{Debug, Formatter};
use std::mem;
use std::sync::Arc;
use anyhow::anyhow;
use core_affinity::CoreId;
use itertools::Itertools;
use rand::prelude::StdRng;
use rand::{RngCore, SeedableRng};
use tokio::time::{Instant, Duration};
use tabled::{Table, Tabled};
use crate::contract::{AtomicFunction, Transaction};
use crate::utils::{mean_ci, mean_ci_str};

const KILO_OFFSET: usize = 10;
const MEGA_OFFSET: usize = 20;
const GIGA_OFFSET: usize = 30;
const KB: usize = 1 << KILO_OFFSET;
const MB: usize = 1 << MEGA_OFFSET;
const GB: usize = 1 << GIGA_OFFSET;

const SEED: u64 = 10;
const MILLION: usize = 1 * 1000 * 1000; // 1M operations per iteration

// =================================================================================================
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
// =================================================================================================

//region Measurements ------------------------------------------------------------------------------
#[derive(Clone)]
struct Measurement {
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

    for _ in 0..nb_iterations {
        let start_iteration = Instant::now();
        let _res = random_step_size_k(array_size_bytes, &mut random_array, nb_operations);
        latencies.push(start_iteration.elapsed());
        // results.push(_res);

        if start.elapsed() > target_duration {
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
    let mut measurements = Vec::with_capacity(nb_cores * nb_sizes);

    let mut core_measurements = vec![vec!(); nb_cores];
    let mut sizes_measurements = vec![vec!(); nb_sizes];

    for core in 0..nb_cores {
        let from_size = adapt_unit((1 << from_power) << KILO_OFFSET);
        let to_size = adapt_unit((1 << to_power) << KILO_OFFSET);
        eprint!("Benchmarking latency to core {} from {} to {}", core, from_size, to_size);

        for (size_index, power_of_two) in (from_power..to_power).enumerate() {
            let array_size_bytes = (1 << power_of_two) << KILO_OFFSET;
            // eprintln!("\n\tarray size = {}", adapt_unit(array_size_bytes));
            let measurement = numa_latency(0, core,
                array_size_bytes, MILLION, 300).await?;

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

    println!("All measurement: =====================================================================");
    for measurement in measurements {
        println!("{}", measurement.to_json());
    }

    Ok(())
}

pub fn transaction_sizes(nb_schedulers: usize, nb_workers: usize) {
    println!("Reference:");
    // println!("\tu32 ~ {}B", mem::size_of::<u32>());
    // println!("\tu64 ~ {}B", mem::size_of::<u64>());
    // println!("\tusize ~ {}B", mem::size_of::<usize>());
    // println!("\tAtomicFunction ~ {}B", mem::size_of::<AtomicFunction>());
    // println!("\tOption<u32> ~ {}B", mem::size_of::<Option<u32>>());
    // println!("\tOption<u64> ~ {}B", mem::size_of::<Option<u64>>());
    // println!("\tOption<usize> ~ {}B", mem::size_of::<Option<usize>>());
    // println!();

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
    // Target chunk sizes that can fill L2 cache?
    // Worker share seems to always be able to fit in L1
    // => most of L2 cache of worker will serve to keep storage hot
}