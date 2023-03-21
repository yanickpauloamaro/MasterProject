use std::cell::RefCell;
use std::io::Write;

use rand::thread_rng;
use rand::seq::SliceRandom;
use hwloc::Topology;
// use hwloc::{Topology, CPUBIND_PROCESS, TopologyObject, ObjectType};
use anyhow::{anyhow, Context, Result};
use std::time::{Duration, Instant};
use std::mem;
use std::ops::{Add, Div};
use async_trait::async_trait;
use bloomfilter::Bloom;
use num_traits::FromPrimitive;
use tokio::runtime::Runtime;
use crate::utils::{
    batch_account_creation,
    batch_partitioned,
    batch_transfer_loop,
    batch_with_conflicts,
    compatible,
    get_nb_nodes,
    print_conflict_rate,
    print_throughput,
};
use crate::vm::{Batch, ExecutionResult, Executor, Jobs};
use crate::config::{BenchmarkConfig, BenchmarkResult, ConfigFile, RunParameter};
use crate::transaction::{Instruction, Transaction, TransactionAddress};
use crate::vm_implementation::{VMa, VMb, VMc, VmFactory, VmType};
use crate::wip::Word;
use crate::worker_implementation::{WorkerBStd, WorkerBTokio};


pub fn benchmarking(path: &str) -> Result<()> {

    let config = BenchmarkConfig::new(path)
        .context("Unable to create benchmark config")?;

    let mut results = vec!();

    let repetitions = config.repetitions;

    for vm_type in config.vm_types.iter() {
        for nb_core in config.nb_cores.iter() {
            for batch_size in config.batch_sizes.iter() {
                let memory_size= 2 * batch_size;
                for conflict_rate in config.conflict_rates.iter() {
                    let parameter = RunParameter::new(
                        *vm_type,
                        *nb_core,
                        *batch_size,
                        memory_size,
                        *conflict_rate,
                        repetitions,
                    );

                    let result = bench_with_parameter(parameter);

                    results.push(result);
                }
            }
        }
    }

    println!("Benchmark end:");
    for result in results {
        println!("{:#?}", result);
    }

    Ok(())
}

fn bench_with_parameter(run: RunParameter) -> BenchmarkResult {

    let vm = RefCell::new(
        VmFactory::new_vm(&run.vm_type, run.memory_size, run.nb_core, run.batch_size)
    );

    let mut latency_reps = vec!();
    let mut throughput_reps = vec!();

    for _ in 0..run.repetitions {
        let batch = batch_with_conflicts(run.batch_size, run.conflict_rate);
        vm.borrow_mut().set_memory(200);

        let start = Instant::now();
        let _vm_output = vm.borrow_mut().execute(batch);
        let duration = start.elapsed();

        let micro_throughput = (run.batch_size as f64).div(duration.as_micros() as f64);
        throughput_reps.push(micro_throughput);
        latency_reps.push(duration);
    }

    // TODO confidence interval in separate function
    let mean_throughput = throughput_reps.iter()
        .fold(0.0, |a, b| a + b).div(run.repetitions as f64);
    let throughput_up = 0.0;
    let throughput_low = 0.0;

    let mean_latency = latency_reps.iter()
        .fold(Duration::from_micros(0), |a, b| a.add(*b)).div(run.repetitions as u32);
    let latency_up = 0.0;
    let latency_low = 0.0;

    let result = BenchmarkResult{
        parameters: run,
        throughput_ci_up: throughput_up,
        throughput_micro: mean_throughput,
        throughput_ci_low: throughput_low,
        latency_ci_up: latency_up,
        latency: mean_latency,
        latency_ci_low: latency_low,
    };

    return result;
}