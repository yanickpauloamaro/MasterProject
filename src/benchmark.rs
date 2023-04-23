use std::cell::RefCell;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use rand::rngs::StdRng;
use rand::SeedableRng;

use crate::config::{BenchmarkConfig, BenchmarkResult, ConfigFile, RunParameter};
use crate::contract::Transaction;
use crate::parallel_vm::{ParallelVmCollect, ParallelVmImmediate};
use crate::sequential_vm::SequentialVM;
use crate::utils::{batch_with_conflicts_new_impl, mean_ci};
use crate::vm::Executor;
use crate::vm_utils::{VmFactory, VmType};
use crate::wip::Word;

pub fn benchmarking(path: &str) -> Result<()> {

    let config = BenchmarkConfig::new(path)
        .context("Unable to create benchmark config")?;

    let mut results = vec!();

    eprintln!("Benchmarking... ");
    let benchmark_start = Instant::now();
    for vm_type in config.vm_types.iter() {
        println!("{:?} {{", vm_type);
        for nb_schedulers in config.nb_schedulers.iter() {
            for nb_executors in config.nb_executors.iter() {
                println!("\t{} schedulers, {} executors {{", nb_schedulers, nb_executors);
                for batch_size in config.batch_sizes.iter() {
                    let storage_size = 100 * batch_size;    // TODO
                    for workload in config.workloads.iter() {
                        let parameter = RunParameter::new(
                            *vm_type,
                            *nb_schedulers,
                            *nb_executors,
                            *batch_size,
                            storage_size,
                            *workload,
                            config.repetitions,
                            config.warmup,
                            config.seed,
                        );

                        let result = if vm_type.new() {
                            if *vm_type == VmType::Sequential {
                                bench_with_parameter_new(parameter)
                            } else {
                                bench_with_parameter_and_details(parameter)
                            }
                        } else {
                            bench_with_parameter(parameter)
                        };

                        println!("\t\t{} {{", workload);

                        println!("\t\t\t{:.2} ± {:.2} tx/µs", result.throughput_micro, result.throughput_ci);
                        print!("\t\t\t{:?} ± {:?}", result.latency, result.latency_ci);
                        if let Some((scheduling, execution)) = &result.latency_breakdown {
                            // print!("\t({}, {})", scheduling, execution);
                            print!("\t(scheduling: {}, execution: {})", scheduling, execution);
                        }
                        println!();
                        println!("\t\t}}");

                        results.push(result);
                    }
                }
                println!("\t}}");
            }
        }
        println!("}}");
    }

    println!("Done. Took {:.2?}", benchmark_start.elapsed());
    println!();
    // for result in results {
    //     println!("{}", result);
    // }

    Ok(())
}

fn bench_with_parameter(run: RunParameter) -> BenchmarkResult {

    let vm = RefCell::new(VmFactory::from(&run));

    let mut latency_reps = Vec::with_capacity(run.repetitions as usize);

    let mut rng = match run.seed {
        Some(seed) => StdRng::seed_from_u64(seed),
        None => StdRng::seed_from_u64(rand::random())
    };

    let batch = batch_with_conflicts_new_impl(
        run.storage_size,
        run.batch_size,
        0.0,//run.workload,// TODO adapt to new contracts
        &mut rng
    );

    for _ in 0..run.warmup {
        let batch = batch.clone();
        vm.borrow_mut().set_storage(200);
        let _vm_output = vm.borrow_mut().execute(batch);
    }

    for _ in 0..run.repetitions {
        let batch = batch.clone();
        // let batch = batch_with_conflicts_new_impl(
        //     run.storage_size,
        //     run.batch_size,
        //     run.conflict_rate,
        //     &mut rng
        // );
        vm.borrow_mut().set_storage(200);

        let start = Instant::now();
        let _vm_output = vm.borrow_mut().execute(batch);
        let duration = start.elapsed();

        latency_reps.push(duration);
    }

    return BenchmarkResult::from_latency(run, latency_reps);
}

fn bench_with_parameter_new(run: RunParameter) -> BenchmarkResult {

    let mut vm = Bench::from(&run);

    let mut latency_reps = Vec::with_capacity(run.repetitions as usize);

    let mut rng = match run.seed {
        Some(seed) => StdRng::seed_from_u64(seed),
        None => StdRng::seed_from_u64(rand::random())
    };

    let batch = run.workload.new_batch(&run, &mut rng);
    for _ in 0..run.warmup {
        let batch = batch.clone();
        vm.set_storage(200);
        let _vm_output = vm.execute(batch);
    }

    for _ in 0..run.repetitions {
        // let batch = Bench::next_batch(&run, &mut rng);
        let batch = batch.clone();
        vm.set_storage(200);

        let start = Instant::now();
        let _vm_output = vm.execute(batch);
        let duration = start.elapsed();

        latency_reps.push(duration);
    }

    return BenchmarkResult::from_latency(run, latency_reps);
}

fn bench_with_parameter_and_details(run: RunParameter) -> BenchmarkResult {

    let mut vm = Bench::from(&run);

    let mut latency_reps = Vec::with_capacity(run.repetitions as usize);
    let mut scheduling_latency = Vec::with_capacity(run.repetitions as usize);
    let mut execution_latency = Vec::with_capacity(run.repetitions as usize);

    let mut rng = match run.seed {
        Some(seed) => StdRng::seed_from_u64(seed),
        None => StdRng::seed_from_u64(rand::random())
    };

    let batch = run.workload.new_batch(&run, &mut rng);
    for _ in 0..run.warmup {
        let batch = batch.clone();
        vm.set_storage(200);
        let _vm_output = vm.execute(batch);
    }

    for _ in 0..run.repetitions {
        // let batch = run.workload.new_batch(&run, &mut rng);
        let batch = batch.clone();

        vm.set_storage(200);
        let start = Instant::now();
        let (scheduling, execution) = vm.execute(batch).unwrap();
        let duration = start.elapsed();

        latency_reps.push(duration);
        scheduling_latency.push(scheduling);
        execution_latency.push(execution);
    }

    return BenchmarkResult::from_latency_with_breakdown(run, latency_reps, scheduling_latency, execution_latency);
}

enum Bench {
    Sequential(SequentialVM),
    ParallelCollect(ParallelVmCollect),
    ParallelImmediate(ParallelVmImmediate),
}

impl Bench {
    pub fn from(p: &RunParameter) -> Self {
        match p.vm_type {
            VmType::Sequential => Bench::Sequential(SequentialVM::new(p.storage_size).unwrap()),
            VmType::ParallelCollect => Bench::ParallelCollect(ParallelVmCollect::new(p.storage_size, p.nb_schedulers, p.nb_executors).unwrap()),
            VmType::ParallelImmediate => Bench::ParallelImmediate(ParallelVmImmediate::new(p.storage_size, p.nb_schedulers, p.nb_executors).unwrap()),
            _ => todo!()
        }
    }

    pub fn set_storage(&mut self, value: Word) {
        match self {
            Bench::Sequential(vm) => vm.set_storage(value),
            Bench::ParallelCollect(vm) => vm.set_storage(value),
            Bench::ParallelImmediate(vm) => vm.set_storage(value),
            _ => todo!()
        }
    }

    pub fn execute(&mut self, batch: Vec<Transaction>) -> Result<(Duration, Duration)>{
        match self {
            Bench::Sequential(vm) => { vm.execute(batch) },
            Bench::ParallelCollect(vm) => { vm.execute(batch) },
            Bench::ParallelImmediate(vm) => { vm.execute(batch) },
            _ => todo!()
        }
    }
}
