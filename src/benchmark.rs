use std::cell::{Cell, RefCell};
use std::error::Error;
use std::{cmp, fmt, mem};
use std::fmt::{Debug, Write};
use std::str::FromStr;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use itertools::Itertools;
use nom::branch::alt;
use nom::IResult;
use nom::bytes::complete::{is_a, take_till, take_until};
use nom::character::complete::{alpha1, char, digit1, one_of};
use nom::combinator::{map_res, rest};
use nom::number::complete::double;
use nom::sequence::{delimited, terminated};
use nom::sequence::Tuple;
use rand::distributions::WeightedIndex;
use rand::prelude::{Distribution, IteratorRandom, SliceRandom};
use rand::rngs::StdRng;
use rand::SeedableRng;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde::de::Visitor;
use thincollections::thin_map::ThinMap;
use thincollections::thin_set::ThinSet;

use crate::applications::Workload;
use crate::{auction, d_hash_map};
use crate::auction::SimpleAuction;
use crate::config::{BenchmarkConfig, BenchmarkResult, ConfigFile, RunParameter};
use crate::contract::{AtomicFunction, FunctionParameter, SenderAddress, SharedMap, StaticAddress, Transaction};
use crate::d_hash_map::{DHashMap, PiecedOperation};
use crate::key_value::{Value, KeyValue, KeyValueOperation};
use crate::micro_benchmark::adapt_unit;
use crate::parallel_vm::{ParallelVmCollect, ParallelVmImmediate};
use crate::sequential_vm::SequentialVM;
use crate::utils::{batch_with_conflicts_new_impl, mean_ci_str};
use crate::vm::Executor;
use crate::vm_utils::{Coordinator, CoordinatorMixed, VmFactory, VmResult, VmType};
use crate::wip::{NONE_WIP, Word};

pub fn benchmarking(path: &str) -> Result<()> {

    eprintln!("/!\\ Using old workload generation with fixed (larger) size transactions, this can lead to slower performance! /!\\");

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
                            workload.clone(),
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

    let workload = Workload::from_str(run.workload.as_str()).unwrap();

    let mut latency_reps = Vec::with_capacity(run.repetitions as usize);

    let mut rng = match run.seed {
        Some(seed) => StdRng::seed_from_u64(seed),
        None => StdRng::seed_from_u64(rand::random())
    };

    let batch = workload.new_batch(&run, &mut rng);
    for _ in 0..run.warmup {
        let batch = batch.clone();
        vm.init_vm_storage(&run);
        let _vm_output = vm.execute(batch);
    }

    for _ in 0..run.repetitions {
        // let batch = Bench::next_batch(&run, &mut rng);
        let batch = batch.clone();
        vm.init_vm_storage(&run);

        let start = Instant::now();
        let _vm_output = vm.execute(batch);
        let duration = start.elapsed();

        latency_reps.push(duration);
    }

    return BenchmarkResult::from_latency(run, latency_reps);
}

fn bench_with_parameter_and_details(run: RunParameter) -> BenchmarkResult {

    let mut vm = Bench::from(&run);
    let workload = Workload::from_str(run.workload.as_str()).unwrap();

    let mut latency_reps = Vec::with_capacity(run.repetitions as usize);
    let mut scheduling_latency = Vec::with_capacity(run.repetitions as usize);
    let mut execution_latency = Vec::with_capacity(run.repetitions as usize);

    let mut rng = match run.seed {
        Some(seed) => StdRng::seed_from_u64(seed),
        None => StdRng::seed_from_u64(rand::random())
    };

    let batch = workload.new_batch(&run, &mut rng);
    for _ in 0..run.warmup {
        let batch = batch.clone();
        vm.init_vm_storage(&run);
        let _vm_output = vm.execute(batch);
    }

    for _ in 0..run.repetitions {
        // let batch = run.workload.new_batch(&run, &mut rng);
        let batch = batch.clone();

        vm.init_vm_storage(&run);
        // let start = Instant::now();
        // let (scheduling, execution) = vm.execute(batch).unwrap();
        // let duration = start.elapsed();
        //
        // latency_reps.push(duration);
        // scheduling_latency.push(scheduling);
        // execution_latency.push(execution);

        let start = Instant::now();
        let result = vm.execute(batch).unwrap();
        let duration = start.elapsed();
        // eprintln!("Done one iteration=======================");
        latency_reps.push(duration);
        scheduling_latency.push(result.scheduling_duration);
        execution_latency.push(result.execution_duration);
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

    pub fn init_vm_storage(&mut self, run: &RunParameter) {
        match self {
            Bench::Sequential(vm) => vm.set_storage(200),
            Bench::ParallelCollect(vm) => vm.set_storage(200),
            Bench::ParallelImmediate(vm) => vm.set_storage(200),
            _ => todo!()
        }
    }

    pub fn execute<const A: usize, const P: usize>(&mut self, batch: Vec<Transaction<A, P>>) -> Result<VmResult<A, P>>{
        match self {
            Bench::Sequential(vm) => { vm.execute(batch) },
            // Bench::Sequential(vm) => { vm.execute_with_results(batch); Ok((Duration::from_secs(0), Duration::from_secs(0))) },
            Bench::ParallelCollect(vm) => { vm.execute(batch) },
            Bench::ParallelImmediate(vm) => { vm.execute(batch) },
            _ => todo!()
        }
    }
}

//region vm wrapper ================================================================================
enum VmWrapper<const A: usize, const P: usize> {
    Sequential(SequentialVM),
    ParallelCollect(ParallelVmCollect),
    ParallelImmediate(ParallelVmImmediate),
    Immediate(Coordinator<A, P>),
    Collect(Coordinator<A, P>),
    Mixed(CoordinatorMixed<A, P>),
}
impl<const A: usize, const P: usize> VmWrapper<A, P> {
    pub fn new(p: &RunParameter) -> Self {
        match p.vm_type {
            VmType::Sequential => VmWrapper::Sequential(SequentialVM::new(p.storage_size).unwrap()),
            VmType::ParallelCollect => VmWrapper::ParallelCollect(ParallelVmCollect::new(p.storage_size, p.nb_schedulers, p.nb_executors).unwrap()),
            VmType::ParallelImmediate => VmWrapper::ParallelImmediate(ParallelVmImmediate::new(p.storage_size, p.nb_schedulers, p.nb_executors).unwrap()),
            VmType::Immediate => VmWrapper::Immediate(Coordinator::new(p.batch_size, p.storage_size, p.nb_schedulers, p.nb_executors)),
            VmType::Collect => VmWrapper::Collect(Coordinator::new(p.batch_size, p.storage_size, p.nb_schedulers, p.nb_executors)),
            VmType::Mixed => VmWrapper::Mixed(CoordinatorMixed::new(p.batch_size, p.storage_size, p.nb_schedulers, p.nb_executors)),
            _ => todo!()
        }
    }
    pub fn init_vm_storage(&mut self, init: Box<dyn Fn(&mut Vec<Word>)>) {
        match self {
            VmWrapper::Sequential(vm) =>  vm.init_storage(init),
            VmWrapper::ParallelCollect(vm) => vm.init_storage(init),
            VmWrapper::ParallelImmediate(vm) => vm.init_storage(init),
            VmWrapper::Immediate(vm) => vm.init_storage(init),
            VmWrapper::Collect(vm) => vm.init_storage(init),
            VmWrapper::Mixed(vm) => vm.init_storage(init),
            _ => todo!()
        }
    }

    pub async fn execute(&mut self, batch: Vec<Transaction<A, P>>) -> Result<VmResult<A, P>> {
        match self {
            VmWrapper::Sequential(vm) => { vm.execute_with_results(batch) },
            VmWrapper::ParallelCollect(vm) => { vm.execute(batch) },
            VmWrapper::ParallelImmediate(vm) => { vm.execute(batch) },
            VmWrapper::Immediate(vm) => {
                // vm.execute(batch, true)
                vm.execute_immediate(batch)
            },
            VmWrapper::Collect(vm) => { vm.execute(batch, false) },
            VmWrapper::Mixed(vm) => { vm.execute(batch).await },
            _ => todo!()
        }
    }

    pub async fn terminate(&mut self) -> (Vec<Duration>, Vec<Duration>) {
        match self {
            VmWrapper::Sequential(vm) => {
                // DHashMap::println::<P>(&vm.storage);
                // DHashMap::print_total_size::<P>(&vm.storage);
                vm.terminate()
            },
            VmWrapper::ParallelCollect(vm) => {
                // DHashMap::print_total_size::<P>(&vm.vm.storage.content);
                vm.terminate()
            },
            VmWrapper::ParallelImmediate(vm) => {
                // DHashMap::println::<P>(&vm.vm.storage.content);
                // DHashMap::print_total_size::<P>(&vm.vm.storage.content);
                vm.terminate()
            },
            VmWrapper::Immediate(vm) => {
                // DHashMap::println::<P>(&vm.storage.content);
                // DHashMap::print_total_size::<P>(&vm.storage.content);
                vm.terminate()
            },
            VmWrapper::Collect(vm) => {
                // DHashMap::print_total_size::<P>(&vm.storage.content);
                vm.terminate()
            },
            VmWrapper::Mixed(vm) => {
                // DHashMap::print_total_size::<P>(&vm.storage.content);
                vm.terminate().await
            },
            _ => (vec!(), vec!())
        }
    }
}
//endregion ========================================================================================

//region TestBench =================================================================================
pub struct TestBench;
impl TestBench {
    pub async fn benchmark(path: &str) -> Result<Vec<BenchmarkResult>> {
        let config = BenchmarkConfig::new(path)
            .context("Unable to create benchmark config")?;

        let mut results = vec!();

        let verbose = true;

        eprintln!("Benchmarking... ");
        let benchmark_start = Instant::now();

        for vm_type in config.vm_types.iter() {
            if verbose { println!("{:?} {{", vm_type); }

            for nb_schedulers in config.nb_schedulers.iter() {
                for nb_executors in config.nb_executors.iter() {
                    if verbose { println!("\t{} schedulers, {} executors {{", nb_schedulers, nb_executors); }

                    for batch_size in config.batch_sizes.iter() {
                        // TODO add storage size to config file
                        // let storage_size = 200 * batch_size;
                        let storage_size = 100 * batch_size;

                        for workload in config.workloads.iter() {
                            let parameter = RunParameter::new(
                                *vm_type,
                                *nb_schedulers,
                                *nb_executors,
                                *batch_size,
                                storage_size,
                                workload.clone(),
                                config.repetitions,
                                config.warmup,
                                config.seed,
                            );

                            results.push(TestBench::run(parameter).await);
                        }
                    }
                    if verbose { println!("\t}}"); }
                }
            }
            if verbose { println!("}}"); }
        }

        println!("Done. Took {:.2?}", benchmark_start.elapsed());
        println!();

        Ok(results)
    }

    async fn run(params: RunParameter) -> BenchmarkResult {

        // TODO Add error handling
        let parsed = Self::parser(params.workload.as_str());

        match parsed {
            Ok(("", (Fib::NAME, args))) => {
                let workload = Fib::new_boxed(&params, args);
                TestBench::dispatch(params, workload).await
            },
            Ok(("", (Transfer::NAME, args))) => {
                let workload = Transfer::new_boxed(&params, args);
                TestBench::dispatch(params, workload).await
            },
            Ok(("", (TransferPieces::NAME, args))) => {
                let workload = TransferPieces::new_boxed(&params, args);
                TestBench::dispatch(params, workload).await
            },
            Ok(("", (KeyValueWorkload::NAME, args))) => {
                let workload = KeyValueWorkload::new_boxed(&params, args);
                TestBench::dispatch(params, workload).await
            },
            Ok(("", (AuctionWorkload::NAME, args))) => {
                let workload = AuctionWorkload::new_boxed(&params, args);
                TestBench::dispatch(params, workload).await
            },
            Ok(("", (name, args))) if name == DHashMapWorkload::NAME ||
                    name == DHashMapWorkload::PIECED ||
                    name == DHashMapWorkload::PREVIOUS =>
                {
                let parse_result = DHashMapWorkload::initial_parser(args);
                if let Ok((other_args, value_size)) = parse_result {
                    let workload = DHashMapWorkload::new_boxed(&params, other_args, value_size, name);
                    match value_size {
                        1 => TestBench::dispatch::<5, 2>(params, workload).await,
                        7 => TestBench::dispatch::<5, 8>(params, workload).await,   // 1 cache line
                        15 => TestBench::dispatch::<5, 16>(params, workload).await, // 2 cache lines
                        23 => TestBench::dispatch::<5, 24>(params, workload).await, // 3 cache lines
                        other => panic!("DHashMapWorkload not implemented for values of size {}", other)
                    }
                } else {
                    panic!("Unable to parse argument to DHashMapWorkload workload. Parse result: {:?}", parse_result)
                }
            },

            Ok(("", (TransferTest::NAME, args))) => {
                let workload = TransferTest::new_boxed(&params, args);
                TestBench::dispatch(params, workload).await
            },
            other => {
                panic!("Unknown workload: {:?}", other)
            }
        }
    }

    fn parser(input: &str) -> IResult<&str, (&str, &str)> {
        (alpha1, delimited(char('('), take_until(")"), char(')'))).parse(input)
    }

    async fn dispatch<const A: usize, const P: usize>(params: RunParameter, mut workload: Box<dyn ApplicationWorkload<A, P>>) -> BenchmarkResult {

        println!("\t\t{} {{", params.workload);

        let result = match params.vm_type {
            VmType::Sequential => TestBench::bench_with_parameter_new(params, workload).await,
            // VmType::ParallelCollect | VmType::ParallelImmediate if params.with_details => TestBench::bench_with_parameter_and_details(params),
            VmType::ParallelCollect | VmType::ParallelImmediate | VmType::Immediate | VmType::Collect | VmType::Mixed
                => TestBench::bench_with_parameter_new_and_details(params, workload).await,
            _ => TestBench::bench_with_parameter(params)
        };

        println!("\t\t\t{:.2} ± {:.2} tx/µs", result.throughput_micro, result.throughput_ci);
        print!("\t\t\t{:?} ± {:?}", result.latency, result.latency_ci);

        // Print details
        if let Some((scheduling, execution)) = &result.latency_breakdown {
            // print!("\t({}, {})", scheduling, execution);
            print!("\t(scheduling: {}, execution: {})", scheduling, execution);
        }
        println!();
        println!("\t\t}}");

        result
    }

    // TODO Migrate old vms to new contracts model and Workloads
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

    async fn bench_with_parameter_new<const A: usize, const P: usize>(mut params: RunParameter, mut workload: Box<dyn ApplicationWorkload<A, P>>) -> BenchmarkResult {

        params.storage_size = workload.storage_size(&params);
        let mut vm = VmWrapper::new(&params);

        let mut latency_reps = Vec::with_capacity(params.repetitions as usize);

        let mut rng = match params.seed {
            Some(seed) => StdRng::seed_from_u64(seed),
            None => StdRng::seed_from_u64(rand::random())
        };
        // TODO better benchmark with more info about which batch is actually created
        let batch = workload.next_batch(&params, &mut rng);
        for _ in 0..params.warmup {
            let batch = batch.clone();
            vm.init_vm_storage(workload.initialisation(&params, &mut rng));
            let _vm_output = vm.execute(batch).await;
        }

        for _ in 0..params.repetitions {
            // let batch = workload.new_batch(&params, &mut rng);
            let batch = batch.clone();
            vm.init_vm_storage(workload.initialisation(&params, &mut rng));

            let start = Instant::now();
            let _vm_output = vm.execute(batch).await;
            let duration = start.elapsed();

            latency_reps.push(duration);
        }

        vm.terminate().await;

        return BenchmarkResult::from_latency(params, latency_reps);
    }

    async fn bench_with_parameter_new_and_details<const A: usize, const P: usize>(mut params: RunParameter, mut workload: Box<dyn ApplicationWorkload<A, P>>) -> BenchmarkResult {

        params.storage_size = workload.storage_size(&params);
        let mut vm = VmWrapper::new(&params);

        let mut latency_reps = Vec::with_capacity(params.repetitions as usize);
        let mut scheduling_latency = Vec::with_capacity(params.repetitions as usize);
        let mut execution_latency = Vec::with_capacity(params.repetitions as usize);

        let mut coordinator_wait = Vec::with_capacity(params.repetitions as usize);
        let mut scheduler_msg_allocation = Vec::with_capacity(params.repetitions as usize);
        let mut scheduler_msg_sending = Vec::with_capacity(params.repetitions as usize);
        let mut executor_msg_allocation = Vec::with_capacity(params.repetitions as usize);
        let mut executor_msg_sending = Vec::with_capacity(params.repetitions as usize);

        let mut rng = match params.seed {
            Some(seed) => StdRng::seed_from_u64(seed),
            None => StdRng::seed_from_u64(rand::random())
        };

        let batch = workload.next_batch(&params, &mut rng);
        for _ in 0..params.warmup {
            let batch = batch.clone();
            vm.init_vm_storage(workload.initialisation(&params, &mut rng));
            let _vm_output = vm.execute(batch).await;
        }

        for _ in 0..params.repetitions {
            // let batch = workload.new_batch(&params, &mut rng);
            let batch = batch.clone();
            vm.init_vm_storage(workload.initialisation(&params, &mut rng));

            // let start = Instant::now();
            // let (scheduling, execution) = vm.execute(batch).unwrap();
            // let duration = start.elapsed();
            //
            // latency_reps.push(duration);
            // scheduling_latency.push(scheduling);
            // execution_latency.push(execution);

            let start = Instant::now();
            let result = vm.execute(batch).await.unwrap();
            let duration = start.elapsed();
            // eprintln!("Done one iteration=======================");
            latency_reps.push(duration);
            scheduling_latency.push(result.scheduling_duration);
            execution_latency.push(result.execution_duration);
            if let Some(wait) = result.coordinator_wait_duration {
                coordinator_wait.push(wait);
            }

            if let Some(wait) = result.scheduler_msg_allocation {
                scheduler_msg_allocation.push(wait);
            }
            if let Some(wait) = result.scheduler_msg_sending {
                scheduler_msg_sending.push(wait);
            }
            if let Some(wait) = result.executor_msg_allocation {
                executor_msg_allocation.push(wait);
            }
            if let Some(wait) = result.executor_msg_sending {
                executor_msg_sending.push(wait);
            }
        }

        let _ = vm.terminate().await;
        let wait = if coordinator_wait.is_empty() { None } else {
            println!("\t\t\twaiting for schedules: {}", mean_ci_str(&coordinator_wait));
            Some(coordinator_wait)
        };
        let wait = if scheduler_msg_allocation.is_empty() { None } else {
            println!("\t\t\tallocating msg (to schedulers): {}", mean_ci_str(&scheduler_msg_allocation));
            Some(scheduler_msg_allocation)
        };
        let wait = if scheduler_msg_sending.is_empty() { None } else {
            println!("\t\t\tsending message to schedulers: {}", mean_ci_str(&scheduler_msg_sending));
            Some(scheduler_msg_sending)
        };
        let wait = if executor_msg_allocation.is_empty() { None } else {
            println!("\t\t\tallocating msg (to executors): {}", mean_ci_str(&executor_msg_allocation));
            Some(executor_msg_allocation)
        };
        let wait = if executor_msg_sending.is_empty() { None } else {
            println!("\t\t\tsending message to executors: {}", mean_ci_str(&executor_msg_sending));
            Some(executor_msg_sending)
        };

        return BenchmarkResult::from_latency_with_breakdown(params, latency_reps, scheduling_latency, execution_latency);
    }
}
//endregion ========================================================================================

//region Workloads =================================================================================
trait ApplicationWorkload<const ADDRESS: usize, const PARAMS: usize> {
    fn next_batch(&mut self, params: &RunParameter, rng: &mut StdRng) -> Vec<Transaction<ADDRESS, PARAMS>>;
    fn initialisation(&self, params: &RunParameter, rng: &mut StdRng) -> Box<dyn Fn(&mut Vec<Word>)>;
    fn storage_size(&self, params: &RunParameter) -> usize {
        100 * params.batch_size
    }
}

//region Fibonacci workload ------------------------------------------------------------------------
struct Fib {
    n: usize,
}

impl Fib {

    const NAME: &'static str = "Fibonacci";

    fn new_boxed(params: &RunParameter, args: &str) -> Box<Self> {
        match usize::from_str(args) {
            Ok(n) => Box::new(Fib{ n }),
            _ => panic!("Unable to parse argument to Fibonacci workload")
        }
    }
}

impl ApplicationWorkload<0, 1> for Fib {
    fn next_batch(&mut self, params: &RunParameter, rng: &mut StdRng) -> Vec<Transaction<0, 1>> {
        (0..params.batch_size).map(|tx_index| {
            Transaction {
                sender: tx_index as SenderAddress,
                function: AtomicFunction::Fibonacci,
                addresses: [],
                tx_index,
                params: [self.n as FunctionParameter],
            }
        }).collect()
    }

    fn initialisation(&self, _params: &RunParameter, _rng: &mut StdRng) -> Box<dyn Fn(&mut Vec<Word>)> {
        // Nothing to do
        Box::new(|storage: &mut Vec<Word>| {  })
    }
}
//endregion

//region Transfer workload -------------------------------------------------------------------------
struct Transfer {
    conflict_rate: f64,
}
impl Transfer {
    const NAME: &'static str = "Transfer";

    fn new_boxed(params: &RunParameter, args: &str) -> Box<Self> {
        match f64::from_str(args) {
            Ok(conflict_rate) => Box::new(Transfer{ conflict_rate }),
            _ => panic!("Unable to parse argument to Transfer workload")
        }
    }
}

impl ApplicationWorkload<2, 1> for Transfer {
    fn next_batch(&mut self, params: &RunParameter, rng: &mut StdRng) -> Vec<Transaction<2, 1>> {
        WorkloadUtils::transfer_pairs(params.storage_size, params.batch_size, self.conflict_rate, rng)
            .iter()
            .enumerate()
            .map(|(tx_index, pair)| {
                Transaction {
                    sender: pair.0 as SenderAddress,
                    function: AtomicFunction::Transfer,
                    tx_index,
                    addresses: [pair.0, pair.1],
                    params: [2],
                }
            }).collect()
    }

    fn initialisation(&self, params: &RunParameter, _rng: &mut StdRng) -> Box<dyn Fn(&mut Vec<Word>)> {

        let nb_repetitions = params.repetitions;
        Box::new(move |storage: &mut Vec<Word>| {
            storage.fill(20 * nb_repetitions as Word)
        })
    }
}
//endregion

//region TransferTest workload -------------------------------------------------------------------------
struct TransferTest {
    conflict_rate: f64,
}
impl TransferTest {
    const NAME: &'static str = "TransferTest";

    fn new_boxed(params: &RunParameter, args: &str) -> Box<Self> {
        match f64::from_str(args) {
            Ok(conflict_rate) => Box::new(TransferTest{ conflict_rate }),
            _ => panic!("Unable to parse argument to TransferTest workload")
        }
    }
}

impl ApplicationWorkload<2, 1> for TransferTest {
    fn next_batch(&mut self, params: &RunParameter, rng: &mut StdRng) -> Vec<Transaction<2, 1>> {
        WorkloadUtils::transfer_pairs(50 * params.batch_size, params.batch_size, self.conflict_rate, rng)
            .iter()
            .enumerate()
            .map(|(tx_index, pair)| {
                Transaction {
                    sender: pair.0 as SenderAddress,
                    function: AtomicFunction::TransferTest,
                    tx_index,
                    addresses: [pair.0, pair.1],
                    params: [2],
                }
            }).collect()
    }

    fn initialisation(&self, params: &RunParameter, rng: &mut StdRng) -> Box<dyn Fn(&mut Vec<Word>)> {

        let nb_repetitions = params.repetitions;
        let nb_accounts = 50 * params.batch_size;
        let capacity = params.storage_size * mem::size_of::<Word>();
        // eprintln!("max account = {}", nb_accounts);
        // println!("capacity = {}, storage_size = {}", capacity, storage_size);

        Box::new(move |storage: &mut Vec<Word>| unsafe {
            storage[0] = nb_accounts as Word;
            let map_start = (storage.as_mut_ptr().add(1)) as *mut Option<Word>;
            let mut shared_map = SharedMap::new(
                Cell::new(map_start),
                nb_accounts,
                capacity);

            for key in 0..nb_accounts {
                unsafe {
                    shared_map.insert(key as StaticAddress, 20 * nb_repetitions as Word);
                }
            }
        })
    }
}
//endregion

//region TransferPieces workload -------------------------------------------------------------------
struct TransferPieces {
    conflict_rate: f64,
}
impl TransferPieces {
    const NAME: &'static str = "TransferPiece";

    fn new_boxed(params: &RunParameter, args: &str) -> Box<Self> {
        match f64::from_str(args) {
            Ok(conflict_rate) => Box::new(TransferPieces{ conflict_rate }),
            _ => panic!("Unable to parse argument to Transfer workload")
        }
    }
}

impl ApplicationWorkload<1, 2> for TransferPieces {
    fn next_batch(&mut self, params: &RunParameter, rng: &mut StdRng) -> Vec<Transaction<1, 2>> {
        WorkloadUtils::transfer_pairs(params.storage_size, params.batch_size, self.conflict_rate, rng)
            .iter()
            .enumerate()
            .map(|(tx_index, pair)| {
                Transaction {
                    sender: pair.0 as SenderAddress,
                    function: AtomicFunction::TransferDecrement,
                    tx_index,
                    addresses: [pair.0],
                    params: [2, pair.1],
                }
            }).collect()
    }

    fn initialisation(&self, params: &RunParameter, rng: &mut StdRng) -> Box<dyn Fn(&mut Vec<Word>)> {
        let nb_repetitions = params.repetitions;
        Box::new(move |storage: &mut Vec<Word>| { storage.fill(20 * nb_repetitions as Word) })
    }
}
//endregion

//region DHashMap workload -------------------------------------------------------------------------
struct DHashMapWorkload {
    bucket_capacity_elems: u32,
    nb_buckets: u32,
    key_space: Vec<FunctionParameter>,
    pieced: bool,
    entry_size: usize,
    map_size: fn(usize, usize, usize)->usize,
    get_proportion: f64,
    insert_proportion: f64,
    remove_proportion: f64,
    contains_proportion: f64,
    // key_distribution: ??? uniform, or zipfian
}

impl DHashMapWorkload {
    const NAME: &'static str = "DHashMap";
    const PIECED: &'static str = "PieceDHashMap";
    const PREVIOUS: &'static str = "PreviousDHashMap";
    // "PieceDHashMap(7, 10, 10; 0.2, 0.2, 0.2, 0.2)"

    pub fn initial_parser(input: &str) -> IResult<&str, u32> {
        terminated(map_res(digit1, str::parse), is_a(" ,"))(input)
    }

    fn parser(input: &str) -> IResult<&str, (u32, u32, f64, f64, f64, f64)> {
        // TODO use numbers from 0-100 instead of floats to ensure we have exact proportions?
        (
            terminated(map_res(digit1, str::parse), is_a(" ,")),    // bucket_capacity_elems
            terminated(map_res(digit1, str::parse), is_a(" ;")),    // nb_buckets
            terminated(double, is_a(" ,")), // get
            terminated(double, is_a(" ,")), // insert
            terminated(double, is_a(" ,")), // remove
            double, // contains
        ).parse(input)
    }

    fn new_boxed(params: &RunParameter, args: &str, value_size: u32, name: &str) -> Box<Self> {
        // todo!("Need to parse input");
        match DHashMapWorkload::parser(args) {
            Ok((_, (bucket_capacity_elems, nb_buckets, get, insert, remove, contains))) => {
                let sum = get + insert + remove + contains;
                assert!(0.0 <= sum);
                assert!(sum <= 1.0);

                let entry_size = value_size as usize + 1;

                let map_size = |nb_buckets: usize, bucket_capacity_elems: usize, entry_size: usize| {
                    (2 + nb_buckets + nb_buckets * (1 + (bucket_capacity_elems as usize) * entry_size))
                };
                // let mut max_nb_buckets = nb_buckets as usize;
                // while map_size(2 * max_nb_buckets, bucket_capacity_elems as usize, entry_size) < params.storage_size {
                //     max_nb_buckets *= 2;
                // }
                // println!("max nb of buckets: {}", max_nb_buckets);
                // println!("nb of buckets after 4 resize: {}", nb_buckets << 4);
                let max_nb_buckets = (nb_buckets << 4) as usize;

                let max_nb_keys = max_nb_buckets * bucket_capacity_elems as usize;
                // eprintln!("Can store at most {} buckets => {} unique keys", max_nb_buckets, max_nb_keys);
                // eprintln!("\ttakes {}", adapt_unit(mem::size_of::<Word>() * map_size(max_nb_keys)));
                let nb_keys = (2 * max_nb_keys) / 3;
                // eprintln!("-> using {} keys to avoid last resize", nb_keys);
                // eprintln!("\ttakes {}", adapt_unit(mem::size_of::<Word>() * map_size(nb_keys)));

                // println!("Initial map takes {} words (storage has {} words, too much? {})",
                //          map_size(nb_buckets as usize, bucket_capacity_elems as usize, entry_size),
                //          params.storage_size,
                //          map_size(nb_buckets as usize, bucket_capacity_elems as usize, entry_size) > params.storage_size);
                let key_space = (0..nb_keys).map(|key| key as FunctionParameter).collect_vec();

                Box::new(DHashMapWorkload {
                    bucket_capacity_elems,
                    nb_buckets,
                    key_space,
                    pieced: name == Self::PIECED,
                    entry_size: value_size as usize + 1,
                    map_size,
                    get_proportion: get,
                    insert_proportion: insert,
                    remove_proportion: remove,
                    contains_proportion: contains,
                })
            }
            other => panic!("Unable to parse argument to DHashMapWorkload workload: {:?}", other)
        }
    }

    pub fn test_batch<const ENTRY_SIZE: usize>(&mut self, params: &RunParameter, rng: &mut StdRng) -> Vec<Transaction<5, ENTRY_SIZE>> {
        let batch_size = 100;
        eprintln!("Test batch");

        let mut batch: Vec<_> = (0..batch_size)
            .map(|tx_index| {
                let mut params = [0 as FunctionParameter; ENTRY_SIZE];
                for i in 0..ENTRY_SIZE { params[i] = i as FunctionParameter; }
                let key = tx_index;
                params[0] = key as FunctionParameter;
                Transaction {
                    sender: tx_index as SenderAddress,
                    function: AtomicFunction::PieceDHashMap(PiecedOperation::InsertComputeHash),
                    // function: AtomicFunction::DHashMap(Operation::Insert),
                    tx_index,
                    addresses: [0, 0, 0, 0, 0],
                    params,
                }
            }).collect();

        let mut gets: Vec<_> = (0..batch_size)
            .map(|tx_index| {
                let mut params = [0 as FunctionParameter; ENTRY_SIZE];
                for i in 0..ENTRY_SIZE { params[i] = i as FunctionParameter; }
                let key = tx_index;
                params[0] = key as FunctionParameter;
                Transaction {
                    sender: tx_index as SenderAddress,
                    function: AtomicFunction::PieceDHashMap(PiecedOperation::GetComputeHash),
                    // function: AtomicFunction::DHashMap(Operation::Get),
                    tx_index,
                    addresses: [0, 0, 0, 0, 0],
                    params,
                }
            }).collect();

        let mut removes: Vec<_> = [3, 4, 8, 9].into_iter()
            .map(|tx_index| {
                let mut params = [0 as FunctionParameter; ENTRY_SIZE];
                for i in 0..ENTRY_SIZE { params[i] = i as FunctionParameter; }
                let key = tx_index;
                params[0] = key as FunctionParameter;
                Transaction {
                    sender: tx_index as SenderAddress,
                    function: AtomicFunction::PieceDHashMap(PiecedOperation::RemoveComputeHash),
                    // function: AtomicFunction::DHashMap(Operation::Remove),
                    tx_index,
                    addresses: [0, 0, 0, 0, 0],
                    params,
                }
            }).collect();

        // batch.append(&mut gets.clone());
        // batch.append(&mut removes);
        // batch.append(&mut gets.clone());
        // println!("batch: {:?}", batch);

        batch.reverse();

        batch
    }
}

impl<const ENTRY_SIZE: usize> ApplicationWorkload<5, ENTRY_SIZE> for DHashMapWorkload {
    fn next_batch(&mut self, params: &RunParameter, rng: &mut StdRng) -> Vec<Transaction<5, ENTRY_SIZE>> {

        // return self.test_batch(params, rng);

        use d_hash_map::*;
        use AtomicFunction::PieceDHashMap;
        use AtomicFunction::DHashMap;

        let weights = [
            self.get_proportion,
            self.insert_proportion,
            self.remove_proportion,
            self.contains_proportion,
        ];

        // TODO Use a parameter to decide between the two types?
        let (addresses, operations) = match &params.workload {
            name if name.starts_with(Self::NAME) => {
                // println!("Using monolithic version version");
                let addresses = [0, 0, 0, 0, 0];
                let operations = [
                    DHashMap(Operation::Get),
                    DHashMap(Operation::Insert),
                    DHashMap(Operation::Remove),
                    DHashMap(Operation::ContainsKey),
                ];
                (addresses, operations)
            },
            name if name.starts_with(Self::PIECED) => {
                // println!("Using pieced version");
                let addresses = [0, 0, 0, 0, 0];
                let operations = [
                    PieceDHashMap(PiecedOperation::GetComputeHash),
                    PieceDHashMap(PiecedOperation::InsertComputeHash),
                    PieceDHashMap(PiecedOperation::RemoveComputeHash),
                    PieceDHashMap(PiecedOperation::HasComputeHash),
                ];
                (addresses, operations)
            },
            name if name.starts_with(Self::PREVIOUS) => {
                // println!("Using previous pieced version");
                let addresses = [0, 0, 0, 0, 0];
                let operations = [
                    PieceDHashMap(PiecedOperation::GetComputeAndFind),
                    PieceDHashMap(PiecedOperation::InsertComputeAndFind),
                    PieceDHashMap(PiecedOperation::RemoveComputeAndFind),
                    PieceDHashMap(PiecedOperation::HasComputeAndFind),
                ];
                (addresses, operations)
            },
            other => panic!("Unknown workload: {:?}", other)
        };

        let dist2 = WeightedIndex::new(weights).unwrap();

        // let mut set = ThinSet::new();
        let mut tx_params = [0 as FunctionParameter; ENTRY_SIZE];
        // let mut __nb_inserts = 0;
        let batch = (0..params.batch_size).map(|tx_index| {
            let op = operations[dist2.sample(rng)];
            let key = *self.key_space.choose(rng).unwrap_or(&0);
            // if op == PieceDHashMap(PiecedOperation::InsertComputeHash) && !set.contains((&key)) {
            //     __nb_inserts += 1;
            //     set.insert(key);
            // }

            for i in 0..ENTRY_SIZE { tx_params[i] = (ENTRY_SIZE - i) as FunctionParameter; }
            tx_params[0] = key;

            Transaction {
                sender: tx_index as SenderAddress,
                function: op,
                tx_index,
                addresses,
                params: tx_params,
            }
        }).collect();
        // println!("Nb to insert = {}", __nb_inserts);

        batch
    }

    fn initialisation(&self, params: &RunParameter, rng: &mut StdRng) -> Box<dyn Fn(&mut Vec<Word>)> {

        let nb_buckets = self.nb_buckets as usize;
        let bucket_capacity_elems = self.bucket_capacity_elems as usize;
        Box::new(move |storage: &mut Vec<Word>| {

            storage.fill(0);
            DHashMap::init::<ENTRY_SIZE>(storage, nb_buckets, bucket_capacity_elems);
        })
    }

    fn storage_size(&self, params: &RunParameter) -> usize {
        let map_after_4_resize = (self.map_size)((self.nb_buckets << 4) as usize, self.bucket_capacity_elems as usize, self.entry_size);
        cmp::max(map_after_4_resize, params.storage_size)
    }
}
//endregion

//region KeyValue workload -------------------------------------------------------------------------
struct KeyValueWorkload {
    read_proportion: f64,
    write_proportion: f64,
    read_modify_write_proportion: f64,
    scan_proportion: f64,
    insert_proportion: f64,
    key_space: usize,
    // key_distribution: ??? uniform, or zipfian
}
impl KeyValueWorkload {
    const NAME: &'static str = "KeyValue";

    fn parser(input: &str) -> IResult<&str, (f64, f64, f64, f64)> {
        // TODO use numbers from 0-100 instead of floats to ensure we have exact proportions?
        (
            terminated(double, is_a(" ,)")), // read
            terminated(double, is_a(" ,)")), // write
            terminated(double, is_a(" ,)")), // rmw
            double, // scan
        ).parse(input)
    }

    fn new_boxed(params: &RunParameter, args: &str) -> Box<Self> {
        // todo!("Need to parse input");
        match KeyValueWorkload::parser(args) {
            Ok((_, (read, write, rmw, scan))) => {
                let sum = read + write + rmw + scan;
                assert!(0.0 <= sum);
                assert!(sum <= 1.0);
                let insert = 1.0 - sum;

                Box::new(KeyValueWorkload {
                    read_proportion: read,
                    write_proportion: write,
                    read_modify_write_proportion: rmw,
                    scan_proportion: scan,
                    insert_proportion: insert,
                    key_space: 50,
                })
            }
            other => panic!("Unable to parse argument to KeyValue workload: {:?}", other)
        }
    }
}

impl ApplicationWorkload<2, 1> for KeyValueWorkload {

    fn next_batch(&mut self, params: &RunParameter, rng: &mut StdRng) -> Vec<Transaction<2, 1>> {
        /*
            ---- Create batch (all on the same address to force conflict?)
            ---- Implement AtomicFunction (monolithic version)
            ---- Proper test batch
            TODO Create batch (pieced version) -> /!\ addresses Transaction<?, ?>
            TODO Implement AtomicFunction (pieced version)
            ---- Add params
         */
        use AtomicFunction::KeyValue;
        let items = [
            (self.read_proportion, KeyValue(KeyValueOperation::Read)),
            (self.write_proportion, KeyValue(KeyValueOperation::Write)),
            (self.read_modify_write_proportion, KeyValue(KeyValueOperation::ReadModifyWrite)),
            (self.scan_proportion, KeyValue(KeyValueOperation::Scan)),
            (self.insert_proportion, KeyValue(KeyValueOperation::Insert)),
        ];
        let dist2 = WeightedIndex::new(items.iter().map(|item| item.0)).unwrap();

        let scan_width = 4;
        let write_value = 33;
        let insert_value = 42;
        let unused_parameter = 0 as FunctionParameter;

        let keys: Vec<StaticAddress> = (0..self.key_space * params.batch_size).map(|i| i as StaticAddress).collect();

        let batch = (0..params.batch_size).map(|mut tx_index| {
            let mut key = *keys.choose(rng).unwrap_or(&(tx_index as StaticAddress));
            let unique_addr = (params.storage_size + tx_index) as StaticAddress;
            match items[dist2.sample(rng)].1 {
                KeyValue(KeyValueOperation::Read) => {
                    Transaction {
                        sender: key as SenderAddress,
                        function: KeyValue(KeyValueOperation::Read),
                        tx_index,
                        addresses: [key, unique_addr],
                        params: [unused_parameter],
                    }
                },
                KeyValue(KeyValueOperation::Write) => {
                    Transaction {
                        sender: key as SenderAddress,
                        function: KeyValue(KeyValueOperation::Write),
                        tx_index,
                        addresses: [key, unique_addr],
                        params: [write_value],
                    }
                },
                KeyValue(KeyValueOperation::ReadModifyWrite) => {
                    // todo!(How to represent different read-modify-write operations?)
                    Transaction {
                        sender: key as SenderAddress,
                        function: KeyValue(KeyValueOperation::ReadModifyWrite),
                        tx_index,
                        addresses: [key, unique_addr],
                        params: [unused_parameter],
                    }
                },
                KeyValue(KeyValueOperation::Scan) => {
                    // TODO Requires scheduling to be aware of the operation it is scheduling
                    // TODO Add address ranges support
                    // TODO Requires all addresses to have been inserted already

                    if key + scan_width >= params.storage_size as StaticAddress {
                        // Ensure we don't scan over the limit
                        // TODO scan implementation should prevent that themselves...
                        key -= scan_width;
                    }
                    Transaction {
                        sender: key as SenderAddress,
                        function: KeyValue(KeyValueOperation::Scan),
                        tx_index,
                        addresses: [key, key + scan_width],
                        params: [unused_parameter],
                    }
                },
                KeyValue(KeyValueOperation::Insert) => {
                    Transaction {
                        sender: key as SenderAddress,
                        function: KeyValue(KeyValueOperation::Insert),
                        tx_index,
                        addresses: [key, unique_addr],
                        params: [insert_value],
                    }
                },
                _ => { todo!() }
            }
        }).collect();

        // // Debug batches
        // Read only
        // TODO Requires all addresses to have been inserted already
        // let mut batch: Vec<_> = (0..params.batch_size).map(|tx_index| {
        //     let address_to_read = tx_index;
        //     let unused_parameter = 0 as FunctionParameter;
        //     Transaction {
        //         sender: tx_index as SenderAddress,
        //         function: AtomicFunction::KeyValue(KeyValueOperation::Read),
        //         tx_index,
        //         addresses: [0],
        //         params: [address_to_read as FunctionParameter, unused_parameter],
        //     }
        // }).collect();

        // // Write
        // // TODO Requires all addresses to have been inserted already
        // let mut batch: Vec<_> = (0..params.batch_size/2).flat_map(|tx_index| {
        //     let address_to_write = tx_index;
        //     let value_to_write = 42 as FunctionParameter;
        //     let write = Transaction {
        //         sender: tx_index as SenderAddress,
        //         function: AtomicFunction::KeyValue(KeyValueOperation::Write),
        //         tx_index,
        //         addresses: [0],
        //         params: [address_to_write as FunctionParameter, value_to_write],
        //     };
        //
        //     let unused_parameter = 0 as FunctionParameter;
        //     let read = Transaction {
        //         sender: tx_index as SenderAddress,
        //         function: AtomicFunction::KeyValue(KeyValueOperation::Read),
        //         tx_index,
        //         addresses: [0],
        //         params: [address_to_write as FunctionParameter, unused_parameter],
        //     };
        //     [read, write]
        // }).collect();

        batch
    }

    fn initialisation(&self, params: &RunParameter, rng: &mut StdRng) -> Box<dyn Fn(&mut Vec<Word>)> {
        // TODO determine key space based on storage_size
        let key_space = self.key_space * params.batch_size;

        Box::new(move |storage: &mut Vec<Word>| unsafe {
            storage[0] = key_space as Word;
            let nb_elem_in_map = storage[0] as usize;
            let map_start = (storage.as_mut_ptr().add(1)) as *mut Option<Value>;
            let _shared_map = SharedMap::new(
                Cell::new(map_start),
                nb_elem_in_map,
                storage.len() * mem::size_of::<Word>(),
            );
            let mut key_value = KeyValue { inner_map: _shared_map };
            for key in 0..key_space {
                let big_value = Value::new(key as u64);
                key_value.insert(key as StaticAddress, big_value);
                // key_value.insert(key as StaticAddress, key as Word);
            }
        })
    }

    fn storage_size(&self, params: &RunParameter) -> usize {
        // TODO depends on KeyValue field size
        600 * params.batch_size
    }
}
//endregion

//region Auction workload --------------------------------------------------------------------------
struct AuctionWorkload {
    nb_auctions: usize,
    nb_bidders: usize,
}
impl AuctionWorkload {
    const NAME: &'static str = "Auction";

    fn new_boxed(params: &RunParameter, args: &str) -> Box<Self> {
        // todo!("Need to parse input");
        match f64::from_str(args) {
            Ok(_) => Box::new(AuctionWorkload {
                nb_auctions: 1,
                nb_bidders: 10,
            }),
            _ => panic!("Unable to parse argument to Votation workload")
        }
    }
}

impl ApplicationWorkload<2, 2> for AuctionWorkload {
    fn next_batch(&mut self, params: &RunParameter, rng: &mut StdRng) -> Vec<Transaction<2, 2>> {
        /*
            Assume auctions have already started
            TODO Create batch (all on the same address to force conflict? (the address of the subject))
            ---- Implement AtomicFunction (monolithic version)
            TODO Create batch (pieced version) -> /!\ addresses Transaction<?, ?>
            TODO Implement AtomicFunction (pieced version)
            ---- Add params (for now we just have one auction with batch_size bidders and no withdraws)
         */
        let max_bid = 20 * params.batch_size;
        let auction_address = params.batch_size + 2;

        let beneficiary = params.batch_size as StaticAddress;
        let unique_addr = params.storage_size as StaticAddress + beneficiary;
        let mut batch = vec![
            // // This is the last tx executed by the sequential version -> can check that the result is correct
            // Transaction {
            //     sender: beneficiary as SenderAddress,
            //     function: AtomicFunction::Auction(auction::Operation::Close),
            //     tx_index,
            //     addresses: [beneficiary, unique_addr],
            //     params: [0, auction_address as FunctionParameter],
            // }
        ];

        // let nb_withdraw = todo!();
        // batch.extend((0..nb_withdraw).map(|tx_index| {
        //     let bidder = tx_index as StaticAddress;
        //     let unique_addr = 2 * params.storage_size as StaticAddress + bidder;
        //     Transaction {
        //         sender: bidder as SenderAddress,
        //         function: AtomicFunction::Auction(auction::Operation::Withdraw),
        //         tx_index,
        //         // addresses: [bidder, unique_addr],
        //         addresses: [bidder, beneficiary],   // Ensure they all conflict
        //         params: [0, auction_address as FunctionParameter],
        //     }
        // }));
        batch.extend((0..params.batch_size).map(|tx_index| {
            let bidder = tx_index as StaticAddress;
            // let bid = max_bid - tx_index;
            let bid = (0..max_bid+1).choose(rng).unwrap_or(0);
            let unique_addr = params.storage_size as StaticAddress + bidder;
            Transaction {
                sender: bidder as SenderAddress,
                function: AtomicFunction::Auction(auction::Operation::Bid),
                tx_index,
                // addresses: [bidder, unique_addr],
                addresses: [bidder, beneficiary],   // Ensure they all conflict
                params: [bid as FunctionParameter, auction_address as FunctionParameter],
            }
        }));

        batch
    }

    fn initialisation(&self, params: &RunParameter, rng: &mut StdRng) -> Box<dyn Fn(&mut Vec<Word>)> {
        // let nb_repetitions = params.repetitions;
        let storage_size = params.storage_size;
        let bidder_balance = 20 * params.batch_size;

        let nb_bidders = params.batch_size;
        let beneficiary = nb_bidders as StaticAddress;
        let auction_balance_address = beneficiary + 1;
        let auction_address = beneficiary + 2;

        // Create auctions and bidders
        Box::new(move |storage: &mut Vec<Word>| unsafe {

            for account in 0..nb_bidders {
                storage[account] = bidder_balance as Word;
            }
            storage[beneficiary as usize] = 0;
            storage[auction_balance_address as usize] = 0;

            let auction_obj_start = storage.as_mut_ptr().add(auction_address as usize);
            auction_obj_start.write(beneficiary as Word);
            auction_obj_start.add(1).write(auction_balance_address as Word);
            auction_obj_start.add(2).write(0); // end_time
            auction_obj_start.add(3).write(2); // ended == 0 <-> true
            auction_obj_start.add(4).write(beneficiary as Word);  // highest_bidder
            auction_obj_start.add(5).write(0); // highest_bid

            auction_obj_start.add(6).write(nb_bidders as Word);
            let map_start = auction_obj_start.add(7) as *mut Option<Word>;

            // Initializes the all entries to None
            let pending_returns = SharedMap::from_ptr(
                Cell::new(map_start),
                nb_bidders,
                (storage_size - 7) * mem::size_of::<Word>(),
            );

            let mut auction = SimpleAuction {
                beneficiary,
                auction_balance_address,
                end_time: 0,
                ended: false,
                highest_bidder: beneficiary,
                highest_bid: 0,
                pending_returns,
            };

            // println!("Initial auction object: {:?}", auction);
            // println!()
        })
    }
}
//endregion

//region Votation workload -------------------------------------------------------------------------
struct Voting {
    nb_subjects: usize,
    nb_proposals_per_subject: usize,
    nb_voters_per_subject: usize,
}
impl Voting {
    const NAME: &'static str = "Vote";

    fn new_boxed(params: &RunParameter, args: &str) -> Box<Self> {
        // todo!("Need to parse input");
        match f64::from_str(args) {
            Ok(_) => Box::new(Voting {
                nb_subjects: 1,
                nb_proposals_per_subject: 2,
                nb_voters_per_subject: 10,
            }),
            _ => panic!("Unable to parse argument to Votation workload")
        }
    }
}

impl ApplicationWorkload<2, 1> for Voting {
    fn next_batch(&mut self, params: &RunParameter, rng: &mut StdRng) -> Vec<Transaction<2, 1>> {
        /* For now assume that:
            - the subjects are initialized and stored in the vm beforehand
            - the voters are initialized are initialized and stored in the vm beforehand
            => batch consists of vote and delegate calls
            TODO Create batch (all on the same address to force conflict? (the address of the subject))
            TODO Implement AtomicFunction (monolithic version)
            TODO Create batch (pieced version) -> /!\ addresses Transaction<?, ?>
            TODO Implement AtomicFunction (pieced version)
            TODO Add delegation rate param
            TODO Add proposal distribution param
         */
        todo!()
    }

    fn initialisation(&self, params: &RunParameter, rng: &mut StdRng) -> Box<dyn Fn(&mut Vec<Word>)> {
        let nb_repetitions = params.repetitions;
        todo!();
        // Create subjects, proposals and voters
        Box::new(move |storage: &mut Vec<Word>| { storage.fill(20 * nb_repetitions as Word) })
    }
}
//endregion

//region BestFit workload --------------------------------------------------------------------------
struct BestFitWorkload {
    nb_best_fit_problems: usize,
    nb_options_per_problem: usize,
}
impl BestFitWorkload {
    const NAME: &'static str = "BestFit";

    fn new_boxed(params: &RunParameter, args: &str) -> Box<Self> {
        // todo!("Need to parse input");
        match f64::from_str(args) {
            Ok(_) => Box::new(BestFitWorkload {
                nb_best_fit_problems: 1,
                nb_options_per_problem: 100,
            }),
            _ => panic!("Unable to parse argument to Votation workload")
        }
    }
}

impl ApplicationWorkload<2, 1> for BestFitWorkload {
    fn next_batch(&mut self, params: &RunParameter, rng: &mut StdRng) -> Vec<Transaction<2, 1>> {
        todo!()
    }

    fn initialisation(&self, params: &RunParameter, rng: &mut StdRng) -> Box<dyn Fn(&mut Vec<Word>)> {
        let nb_repetitions = params.repetitions;
        todo!();
        Box::new(move |storage: &mut Vec<Word>| {  })
    }
}
//endregion

//endregion ========================================================================================

//region workload utils ============================================================================
pub struct WorkloadUtils;
impl WorkloadUtils {
    pub fn transfer_pairs(memory_size: usize, batch_size: usize, conflict_rate: f64, mut rng: &mut StdRng) -> Vec<(StaticAddress, StaticAddress)> {
        let nb_conflict = (conflict_rate * batch_size as f64).ceil() as usize;

        let mut addresses: Vec<StaticAddress> = (0..memory_size).map(|el| el as StaticAddress).collect();
        addresses.shuffle(&mut rng);
        addresses.truncate(2 * batch_size);

        // let mut addresses: Vec<StaticAddress> = (0..memory_size)
        //     .choose_multiple(&mut rng, 2*batch_size)
        //     .into_iter().map(|el| el as StaticAddress)
        //     .collect();

        let mut receiver_occurrences: ThinMap<StaticAddress, u64> = ThinMap::with_capacity(batch_size);
        let mut batch = Vec::with_capacity(batch_size);

        // Create non-conflicting transactions
        for _ in 0..batch_size {
            let from = addresses.pop().unwrap();
            let to = addresses.pop().unwrap();

            // Ensure senders and receivers don't conflict. Otherwise, would need to count conflicts
            // between senders and receivers
            // to += batch_size as u64;

            receiver_occurrences.insert(to, 1);

            batch.push((from, to));
        }

        let indices: Vec<usize> = (0..batch_size).collect();

        let mut conflict_counter = 0;
        while conflict_counter < nb_conflict {
            let i = *indices.choose(&mut rng).unwrap();
            let j = *indices.choose(&mut rng).unwrap();

            if batch[i].1 != batch[j].1 {

                let freq_i = *receiver_occurrences.get(&batch[i].1).unwrap();
                let freq_j = *receiver_occurrences.get(&batch[j].1).unwrap();

                if freq_j != 2 {
                    if freq_j == 1 { conflict_counter += 1; }
                    if freq_i == 1 { conflict_counter += 1; }

                    receiver_occurrences.insert(batch[i].1, freq_i + 1);
                    receiver_occurrences.insert(batch[j].1, freq_j - 1);

                    batch[j].1 = batch[i].1;
                }
            }
        }

        // WorkloadUtils::print_conflict_rate(&batch);

        batch
    }

    fn print_conflict_rate(batch: &Vec<(StaticAddress, StaticAddress)>) {

        let mut nb_addresses = 0;

        let mut conflicts = ThinMap::new();
        let mut nb_conflicts = 0;
        let mut nb_conflicting_addr = 0;

        for tx in batch.iter() {
            // The 'from' address is always different
            nb_addresses += 1;

            // TODO Make this computation work for arbitrary transfer graphs
            // if addresses.insert(tx.from) {
            //     nb_addresses += 1;
            // }
            // if addresses.insert(tx.addresses[1]) {
            //     nb_addresses += 1;
            // }

            match conflicts.get_mut(&tx.1) {
                None => {
                    conflicts.insert(tx.1, 1);
                    nb_addresses += 1;
                },
                Some(occurrence) if *occurrence == 1 => {
                    *occurrence += 1;
                    nb_conflicts += 2;
                    nb_conflicting_addr += 1;
                    // println!("** {} is appearing for the 2nd time", tx.addresses[1]);

                },
                Some(occurrence) => {
                    *occurrence += 1;
                    nb_conflicts += 1;
                    // println!("** {} is appearing for the {}-th time", tx.addresses[1], *occurrence);
                },
            }
        }

        // Manual check
        // let mut actual_conflicts = 0;
        // let mut actual_nb_conflicting_addr = 0;
        // for (addr, freq) in conflicts {
        //     // println!("{} appears {} times", addr, freq);
        //     if freq > 1 {
        //         actual_nb_conflicting_addr += 1;
        //         actual_conflicts += freq;
        //         println!("** {} appears {} times", addr, freq);
        //     }
        // }
        // println!("Other calculation: nb conflicts {}", actual_conflicts);
        // println!("Other calculation: nb conflict address {}", actual_nb_conflicting_addr);
        // println!();

        let conflict_rate = (nb_conflicts as f64) / (batch.len() as f64);
        let conflict_addr_rate = (nb_conflicting_addr as f64) / (nb_addresses as f64);

        println!("Nb of conflicts: {}/{}", nb_conflicts, batch.len());
        println!("Conflict rate: {:.2}%",  100.0 * conflict_rate);
        println!("Nb conflicting addresses: {}/{}", nb_conflicting_addr, nb_addresses);
        println!("Ratio of conflicting addresses: {:.2}%",  100.0 * conflict_addr_rate);
        println!();
    }
}
//endregion ========================================================================================