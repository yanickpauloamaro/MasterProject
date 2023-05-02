use anyhow::anyhow;
use core_affinity::CoreId;
use rand::prelude::StdRng;
use rand::{RngCore, SeedableRng};
use tokio::time::{Instant, Duration};
use tabled::{Table, Tabled};
use crate::utils::mean_ci_str;

const KILO_OFFSET: usize = 10;
const MEGA_OFFSET: usize = 20;
const GIGA_OFFSET: usize = 30;
const KB: usize = 1 << KILO_OFFSET;
const MB: usize = 1 << MEGA_OFFSET;
const GB: usize = 1 << GIGA_OFFSET;

const SEED: u64 = 10;
const MILLION: usize = 1 * 1000 * 1000; // 1M operations per iteration

// =============================================================================================
// getconf -a | grep CACHE
// Laptop
const L1_SIZE: usize = 32 * KB;    // per core
const L2_SIZE: usize = 256 * KB;   // per core
const L3_SIZE: usize = 6 * MB;     // shared
const CACHE_LINE_SIZE: u8 = 64;

// // AWS?
// let L1_SIZE = 48 * kilo;            // 48 KB (per core)
// let L2_SIZE = 1 * mega + mega/4;    // 1.25 MB (per core)
// let L3_SIZE = 54 * mega;            // 54 MB (shared)
// const CACHE_LINE_SIZE: usize = 64;
// =============================================================================================

#[derive(Tabled)]
struct CacheLatencyMeasurement {
    array_size: String,
    mean_latency: String,
    nb_iterations: usize,
    total_duration: String
}

impl CacheLatencyMeasurement {
    fn new(array_size_b: usize, latencies: &Vec<Duration>, took: Duration) -> Self {
        Self {
            array_size: adapt_unit(array_size_b),
            mean_latency: mean_ci_str(latencies),
            nb_iterations: latencies.len(),
            total_duration: format!("{:.3?}", took)
        }
    }
}

fn adapt_unit(size_b: usize) -> String {
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
        let pos = idx & (size - 1);
        arr[pos] += 1;
        idx += (arr[pos] * CACHE_LINE_SIZE) as usize;
    }

    return idx;
}

pub async fn numa_latency_between(execute_core: usize, memory_core: usize, from_power: usize, to_power: usize) -> anyhow::Result<()> {
    // https://stackoverflow.com/questions/47749905/determine-numa-layout-via-latency-performance-measurements

    eprintln!("Execution core: {}, memory core: {}", execute_core, memory_core);
    // eprintln!("Pinning execution to core {}...", execute_core);
    let pinned = core_affinity::set_for_current(CoreId{ id: execute_core });
    if !pinned {
        return Err(anyhow!("Unable to pin to execution core to CPU #{}", execute_core));
    }

    let size_range = from_power..to_power;
    let sizes: Vec<_> = size_range.map(|power_of_two| (1 << power_of_two) << KILO_OFFSET).collect();
    let iterations: Vec<_> = sizes.iter().map(|size| {
        // match *size {
        //     s if s <= L1_SIZE/KB => 200 as usize,
        //     s if s < L2_SIZE/KB => 150 as usize,
        //     s if s <= L3_SIZE/KB => 100 as usize,
        //     s => 80 as usize,
        // }
        300
    }).collect();

    let from = adapt_unit(*sizes.first().unwrap());
    let to = adapt_unit(*sizes.last().unwrap());

    let mut outputs = Vec::with_capacity(sizes.len());
    let inputs = sizes.into_iter().zip(iterations.into_iter());

    eprint!("Benchmarking from {} to {}", from, to);

    let target_duration = Duration::from_millis(500);
    let start = Instant::now();
    for (array_size, iterations) in inputs {
        let start_k = Instant::now();
        let mut arr = random_data_init(array_size, memory_core).await?;

        let mut latencies = Vec::with_capacity(iterations);
        let mut results = Vec::with_capacity(iterations);

        for _ in 0..iterations {
            let start_iteration = Instant::now();
            let res = random_step_size_k(array_size, &mut arr, MILLION);
            latencies.push(start_iteration.elapsed());
            results.push(res);

            if start_k.elapsed() > target_duration {
                break;
            }
        }

        let took = start_k.elapsed();

        outputs.push(CacheLatencyMeasurement::new(array_size, &latencies, took));
        eprint!(".");
    }
    eprintln!();
    eprintln!("{}", Table::new(outputs).to_string());
    eprintln!("Benchmark took {:.2?}", start.elapsed());
    println!();

    Ok(())
}

pub async fn all_numa_latencies(nb_cores: usize, from_power: usize, to_power: usize) -> anyhow::Result<()> {
    for core in 0..nb_cores {
        numa_latency_between(0, core, from_power, to_power).await?;
    }

    Ok(())
}