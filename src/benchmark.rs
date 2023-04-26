use std::cell::RefCell;
use std::error::Error;
use std::fmt;
use std::fmt::{Debug, Write};
use std::str::FromStr;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use rand::prelude::SliceRandom;
use rand::rngs::StdRng;
use rand::SeedableRng;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde::de::Visitor;
use thincollections::thin_map::ThinMap;
use crate::applications::Workload;

use crate::config::{BenchmarkConfig, BenchmarkResult, ConfigFile, RunParameter};
// use crate::contract::AtomicFunction::Transfer;
use crate::contract::{AtomicFunction, FunctionParameter, SenderAddress, StaticAddress, Transaction};
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
                            workload.clone(),
                            config.repetitions,
                            config.warmup,
                            config.seed,
                        );

                        let result = if vm_type.new() {
                            bench_with_parameter_new(parameter)
                            // if *vm_type == VmType::Sequential {
                            //     bench_with_parameter_new(parameter)
                            // } else {
                            //     bench_with_parameter_and_details(parameter)
                            // }
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
//
// fn bench_with_parameter_and_details(run: RunParameter) -> BenchmarkResult {
//
//     let mut vm = Bench::from(&run);
//
//     let mut latency_reps = Vec::with_capacity(run.repetitions as usize);
//     let mut scheduling_latency = Vec::with_capacity(run.repetitions as usize);
//     let mut execution_latency = Vec::with_capacity(run.repetitions as usize);
//
//     let mut rng = match run.seed {
//         Some(seed) => StdRng::seed_from_u64(seed),
//         None => StdRng::seed_from_u64(rand::random())
//     };
//
//     let batch = run.workload.new_batch(&run, &mut rng);
//     for _ in 0..run.warmup {
//         let batch = batch.clone();
//         vm.init_vm_storage(&run);
//         let _vm_output = vm.execute(batch);
//     }
//
//     for _ in 0..run.repetitions {
//         // let batch = run.workload.new_batch(&run, &mut rng);
//         let batch = batch.clone();
//
//         vm.init_vm_storage(&run);
//         let start = Instant::now();
//         let (scheduling, execution) = vm.execute(batch).unwrap();
//         let duration = start.elapsed();
//
//         latency_reps.push(duration);
//         scheduling_latency.push(scheduling);
//         execution_latency.push(execution);
//     }
//
//     return BenchmarkResult::from_latency_with_breakdown(run, latency_reps, scheduling_latency, execution_latency);
// }

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

    pub fn execute<const A: usize, const B: usize>(&mut self, batch: Vec<Transaction<A, B>>) -> Result<(Duration, Duration)>{
        match self {
            Bench::Sequential(vm) => { vm.execute(batch) },
            Bench::ParallelCollect(vm) => { vm.execute(batch) },
            Bench::ParallelImmediate(vm) => { vm.execute(batch) },
            _ => todo!()
        }
    }
}

enum VmWrapper {
    Sequential(SequentialVM),
    ParallelCollect(ParallelVmCollect),
    ParallelImmediate(ParallelVmImmediate),
}
impl VmWrapper {
    pub fn new(p: &RunParameter) -> Self {
        match p.vm_type {
            VmType::Sequential => VmWrapper::Sequential(SequentialVM::new(p.storage_size).unwrap()),
            VmType::ParallelCollect => VmWrapper::ParallelCollect(ParallelVmCollect::new(p.storage_size, p.nb_schedulers, p.nb_executors).unwrap()),
            VmType::ParallelImmediate => VmWrapper::ParallelImmediate(ParallelVmImmediate::new(p.storage_size, p.nb_schedulers, p.nb_executors).unwrap()),
            _ => todo!()
        }
    }
    pub fn init_vm_storage(&mut self, init: Box<dyn Fn(&mut Vec<Word>)>) {
        match self {
            VmWrapper::Sequential(vm) =>  vm.init_storage(init),
            VmWrapper::ParallelCollect(vm) => vm.init_storage(init),
            VmWrapper::ParallelImmediate(vm) => vm.init_storage(init),
            _ => todo!()
        }
    }

    pub fn execute<const A: usize, const B: usize>(&mut self, batch: Vec<Transaction<A, B>>) -> Result<(Duration, Duration)>{
        match self {
            VmWrapper::Sequential(vm) => { vm.execute(batch) },
            VmWrapper::ParallelCollect(vm) => { vm.execute(batch) },
            VmWrapper::ParallelImmediate(vm) => { vm.execute(batch) },
            _ => todo!()
        }
    }
}

pub struct TestBench;
impl TestBench {
    pub fn benchmark(path: &str) -> Result<Vec<BenchmarkResult>> {
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

                            results.push(TestBench::run(parameter));
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

    fn run(params: RunParameter) -> BenchmarkResult {
        // TODO parse first part of input
        match params.workload.as_str() {
            "Fibonacci(5)" => {
                let workload = Fib::new_boxed(&params);
                TestBench::dispatch(params, workload)
            },
            "Transfer(0.0)" => {
                let workload = Transfer::new_boxed(&params);
                TestBench::dispatch(params, workload)
            },
            "TransferPiece(0.0)" => {
                let workload = TransferPieces::new_boxed(&params);
                TestBench::dispatch(params, workload)
            },
            _ => todo!(),//panic!("Unknown workload")
        }
    }

    fn dispatch<const A: usize, const P: usize>(params: RunParameter, mut workload: Box<dyn ApplicationWorkload<A, P>>) -> BenchmarkResult {

        println!("\t\t{} {{", params.workload);

        let result = match params.vm_type {
            VmType::Sequential => TestBench::bench_with_parameter_new(params, workload),
            // VmType::ParallelCollect | VmType::ParallelImmediate if params.with_details => TestBench::bench_with_parameter_and_details(params),
            VmType::ParallelCollect | VmType::ParallelImmediate => TestBench::bench_with_parameter_new(params, workload),
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

    fn bench_with_parameter_new<const A: usize, const P: usize>(params: RunParameter, mut workload: Box<dyn ApplicationWorkload<A, P>>) -> BenchmarkResult {

        let mut vm = VmWrapper::new(&params);

        let mut latency_reps = Vec::with_capacity(params.repetitions as usize);

        let mut rng = match params.seed {
            Some(seed) => StdRng::seed_from_u64(seed),
            None => StdRng::seed_from_u64(rand::random())
        };

        let batch = workload.next_batch(&params, &mut rng);
        for _ in 0..params.warmup {
            let batch = batch.clone();
            vm.init_vm_storage(workload.initialisation(&params, &mut rng));
            let _vm_output = vm.execute(batch);
        }

        for _ in 0..params.repetitions {
            // let batch = workload.new_batch(&params, &mut rng);
            let batch = batch.clone();
            vm.init_vm_storage(workload.initialisation(&params, &mut rng));

            let start = Instant::now();
            let _vm_output = vm.execute(batch);
            let duration = start.elapsed();

            latency_reps.push(duration);
        }

        return BenchmarkResult::from_latency(params, latency_reps);
    }
}

trait ApplicationWorkload<const ADDRESS: usize, const PARAMS: usize> {
    fn next_batch(&mut self, params: &RunParameter, rng: &mut StdRng) -> Vec<Transaction<ADDRESS, PARAMS>>;
    fn initialisation(&self, params: &RunParameter, rng: &mut StdRng) -> Box<dyn Fn(&mut Vec<Word>)>;
}

//region Fibonacci workload ------------------------------------------------------------------------
struct Fib {
    n: usize,
}

impl Fib {
    fn new_boxed(params: &RunParameter) -> Box<Self> {
        // TODO use parsing
        match usize::from_str("5") {
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
                params: [self.n as FunctionParameter],
            }
        }).collect()
    }

    fn initialisation(&self, params: &RunParameter, rng: &mut StdRng) -> Box<dyn Fn(&mut Vec<Word>)> {
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
    fn new_boxed(params: &RunParameter) -> Box<Self> {
        // todo!("Need to parse input");
        match f64::from_str("0.0") {
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
                    // nb_addresses: 2,
                    addresses: [pair.0, pair.1],
                    params: [2],
                }
            }).collect()
    }

    fn initialisation(&self, params: &RunParameter, rng: &mut StdRng) -> Box<dyn Fn(&mut Vec<Word>)> {
        let nb_repetitions = params.repetitions;
        Box::new(move |storage: &mut Vec<Word>| { storage.fill(20 * nb_repetitions as Word) })
    }
}
//endregion

//region TransferPieces workload -------------------------------------------------------------------
struct TransferPieces {
    conflict_rate: f64,
}
impl TransferPieces {
    fn new_boxed(params: &RunParameter) -> Box<Self> {
        // todo!("Need to parse input");
        match f64::from_str("0.0") {
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

struct WorkloadUtils;
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

        // Workload::print_conflict_rate(&batch);

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

//region draft of correct attempt
struct TestingAgain<const SIZE: usize>;

struct BenchmarkDispatch;

impl BenchmarkDispatch {
    pub fn benchmark(run: RunParameter) -> BenchmarkResult {
        let s = "str";
        // match s {
        //     "Fibonacci(10)" => BenchmarkDispatch::inner_bench_with_parameter_and_details(run, Box::new(FibWorkload{n : 10})),
        //     "Fibonacci(5)" => FibWorkload{n : 5}.bench_with_parameter_and_details(run),
        //     "Transfer(0.0)" => TransferWorkload{ conflict_rate: 0.0 }.bench_with_parameter_and_details(run),
        //     _ => todo!()
        // };
        let workload = match s {
            "Fibonacci(5)" => Box::new(FibWorkload{n : 5}),
            "Fibonacci(10)" => Box::new(FibWorkload{n : 5}),
            // "Transfer(0.0)" => Box::new(TransferWorkload{conflict_rate : 0.0}),
            _ => todo!()
        };

        BenchmarkDispatch::inner_bench_with_parameter_and_details(run, workload)
        // todo!()
    }

    fn inner_bench_with_parameter_and_details<const A: usize, const P: usize>(run: RunParameter, workload: Box<dyn Win<A, P>>) -> BenchmarkResult {

        // VM Wrapper
        let mut vm = Bench::from(&run);
        // let workload = FibWorkload{n : 5};

        let mut latency_reps = Vec::with_capacity(run.repetitions as usize);
        let mut scheduling_latency = Vec::with_capacity(run.repetitions as usize);
        let mut execution_latency = Vec::with_capacity(run.repetitions as usize);

        let mut rng = match run.seed {
            Some(seed) => StdRng::seed_from_u64(seed),
            None => StdRng::seed_from_u64(rand::random())
        };

        let batch = workload.new_batch(/*&run, replaced by &self*/&mut rng);
        for _ in 0..run.warmup {
            let batch = batch.clone();
            vm.init_vm_storage(&run);
            let _vm_output = vm.execute(batch);
        }

        for _ in 0..run.repetitions {
            // let batch = run.workload.new_batch(&run, &mut rng);
            let batch = batch.clone();

            vm.init_vm_storage(&run);
            let start = Instant::now();
            let (scheduling, execution) = vm.execute(batch).unwrap();
            let duration = start.elapsed();

            latency_reps.push(duration);
            scheduling_latency.push(scheduling);
            execution_latency.push(execution);
        }

        return BenchmarkResult::from_latency_with_breakdown(run, latency_reps, scheduling_latency, execution_latency);
    }
}

struct FibWorkload{
    n: usize,
}
impl Win<0, 1> for FibWorkload {
    fn new_batch(&self, rng: &mut StdRng) -> Vec<Transaction<0, 1>> {
        todo!()
    }

    fn init_storage(&self, rng: &mut StdRng, storage: &mut Vec<Word>) {
        todo!()
    }
}

struct TransferWorkload {
    conflict_rate: f64,
}

impl Win<2, 2> for TransferWorkload {
    fn new_batch(&self, rng: &mut StdRng) -> Vec<Transaction<2, 2>> {
        todo!()
    }

    fn init_storage(&self, rng: &mut StdRng, storage: &mut Vec<Word>) {
        todo!()
    }
}

trait Win<const SIZE: usize, const PARAMS: usize> {
    fn new_batch(&self, rng: &mut StdRng) -> Vec<Transaction<SIZE, PARAMS>>;
    fn init_storage(&self, rng: &mut StdRng, storage: &mut Vec<Word>);
}
//endregion

//region attempts
// trait Win<const ADDRESS: usize, const PARAMS: usize>: Debug {
//     // const SIZE: usize;
//     fn new_batch(&self) -> Vec<Transaction<ADDRESS, PARAMS>>;
// }

trait W<const SIZE: usize>: Debug {
    // const SIZE: usize;
    fn new_batch(&self) -> Vec<Transaction<SIZE, SIZE>>;
}

impl<const SIZE: usize> FromStr for Box<dyn W<SIZE>> {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // TODO Implement proper parsing
        let res = match s {
            _ => todo!()
        };

        Ok(res)
    }
}

impl<const SIZE: usize> Serialize for dyn W<SIZE> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error> where S: Serializer, {
        serializer.serialize_str(format!("{:?}", self).as_str())
    }
}

struct WVisitor<const SIZE: usize>;
impl<'de, const SIZE: usize> Visitor<'de> for WVisitor<SIZE> {
    type Value = Box<dyn W<SIZE>>;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("A workload used to benchmark a smart contract VM")
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E> where E: Error {
        let res = Box::<dyn W<SIZE>>::from_str(v).unwrap();
        Ok(res)
    }

    fn visit_string<E>(self, v: String) -> Result<Self::Value, E> where E: Error {
        let res = Box::<dyn W<SIZE>>::from_str(v.as_str()).unwrap();
        Ok(res)
    }
}

impl<'de, const SIZE: usize> Deserialize<'de> for Box<dyn W<SIZE>> {
    fn deserialize<D>(deserializer: D) -> Result<Box<dyn W<SIZE>>, D::Error> where D: Deserializer<'de>, {
        deserializer.deserialize_string(WVisitor)
    }
}

// #[derive(Debug)]
// struct Voting;
// impl W<2> for Voting {
//     fn new_batch(&self) -> [u32; 2] {
//         todo!()
//     }
// }

// trait WorkloadBench<const A: usize, const P: usize> {
//     fn from(p: &RunParameter) -> Self;
//
//     fn init_vm_storage(&mut self, run: &RunParameter);
//
//     fn execute(&mut self, batch: Vec<Transaction<A, P>>) -> Result<(Duration, Duration)>;
// }
//
// struct TestBench;
// impl<const A: usize, const P: usize> WorkloadBench<A, P> for TestBench {
//     fn from(p: &RunParameter) -> Self {
//         todo!()
//     }
//
//     fn init_vm_storage(&mut self, run: &RunParameter) {
//         todo!()
//     }
//
//     fn execute(&mut self, batch: Vec<Transaction<A, P>>) -> Result<(Duration, Duration)> {
//         todo!()
//     }
// }
//
// struct VM{
//     functions: Vec<Atomics>,
//     dynamic: Vec<Box<dyn Application<_, _>>>
// }
// impl VM {
//
//     fn execute<const SIZE: usize>(&self, tx: Tx<SIZE>) {
//         // for f in self.functions.iter() {
//         //     let mut tx = Tx{ content: [0; SIZE]};
//         //     let res = f.execute(tx);
//         // }
//
//         let mut two = vec!();
//         let mut four = vec!();
//         let mut ten = vec!();
//         let f = self.functions.get(tx.function_index).unwrap();
//         // let res = f.execute(tx);
//         match f.output_size() {
//             2 => two.push(f.execute::<SIZE, 2>(tx)),
//             4 => four.push(f.execute::<SIZE, 4>(tx)),
//             10 => ten.push(f.execute::<SIZE, 10>(tx)),
//             _ => todo!()
//         }
//     }
//
//     fn process<const SIZE: usize>(&self, batch: Vec<Tx<SIZE>>) {
//         /* Transactions of any size are ok but all transactions in a batch must have the same size
//             Code can be adapted to batches that have multiple sizes of transactions: we simply need
//             to sort the tx by size and execute each group as a sub batch (out of scope)
//
//             In the meantime, tx of a given applications will have the largest size that can accommodate
//             the application (i.e. if one piece needs 2 addresses but another needs 10, the tx size will be 10)
//          */
//         for tx in batch {
//             self.execute(tx);
//         }
//     }
//
//     fn schedule<const SIZE: usize>(&self, chunk: Vec<Tx<SIZE>>) {
//         /* TODO schedule should use knowledge of the tx type to decide how many addresses inside
//             tx.addresses should be added to the working set
//          */
//     }
// }
//
// struct Tx<const SIZE: usize> {
//     // SIZE must match the function stored at that function_index
//     pub content: [u32; SIZE],
//     pub function_index: usize,
//     // pub tx_index: usize,
// }
//
// impl<const SIZE: usize> Tx<SIZE> {
//     fn test(&self) -> u32 {
//         self.content[SIZE + 1]
//     }
// }
//
// enum Atomics {
//     A(InstructionA),
//     B(InstructionB)
// }
// impl Atomics {
//     fn execute<const SIZE_IN: usize, const SIZE_OUT: usize>(&self, tx: Tx<SIZE_IN>) -> Tx<SIZE_OUT> {
//         match self {
//             Atomics::A(instr) => instr.execute(tx),
//             Atomics::B(instr) => instr.execute(tx)
//         }
//     }
//
//     fn input_size(&self) -> usize {
//         match self {
//             Atomics::A(instr) => instr.input_size(),
//             Atomics::B(instr) => instr.input_size()
//         }
//     }
//
//     fn output_size(&self) -> usize {
//         match self {
//             Atomics::A(instr) => instr.output_size(),
//             Atomics::B(instr) => instr.output_size()
//         }
//     }
// }
//
// enum InstructionA {
//     First,
//     Second,
// }
//
// impl InstructionA {
//     fn execute<const SIZE_IN: usize, const SIZE_OUT: usize>(&self, mut tx: Tx<SIZE_IN>) -> Tx<SIZE_OUT> {
//         match self {
//             InstructionA::First => {
//                 assert_eq!(SIZE_IN, 10);
//                 assert_eq!(SIZE_OUT, 4);
//                 Tx{ content: [0; SIZE_OUT], function_index: 0}
//             },
//             InstructionA::Second => todo!()
//         }
//     }
//
//     fn input_size(&self) -> usize {
//         match self {
//             InstructionA::First => 10,
//             InstructionA::Second => 4
//         }
//     }
//
//     fn output_size(&self) -> usize {
//         match self {
//             InstructionA::First => 4,
//             InstructionA::Second => 4
//         }
//     }
// }
//
// enum InstructionB {
//     Foo,
//     Bar,
// }
//
// impl InstructionB {
//     fn execute<const SIZE_IN: usize, const SIZE_OUT: usize>(&self, tx: Tx<SIZE_IN>) -> Tx<SIZE_OUT> {
//         assert_eq!(SIZE_IN, 2);
//         match self {
//             InstructionB::Foo => todo!(),
//             InstructionB::Bar => todo!()
//         }
//     }
//
//     fn input_size(&self) -> usize {
//         match self {
//             InstructionB::Foo => 2,
//             InstructionB::Bar => 4
//         }
//     }
//
//     fn output_size(&self) -> usize {
//         match self {
//             InstructionB::Foo => 2,
//             InstructionB::Bar => 4
//         }
//     }
// }
// ====
//
// trait Application {
//     fn execute<const SIZE_IN: usize, const SIZE_OUT: usize>(&self, tx: Tx<SIZE_IN>) -> Tx<SIZE_OUT>;
// }
//
// trait AppC<const SIZE_IN: usize, const SIZE_OUT: usize>: Sized + Application {
//     // fn execute(&self, tx: Tx<SIZE_IN>) -> Tx<SIZE_OUT>;
// }
// struct FirstInstructionC;
// impl Application for FirstInstructionC {
//     fn execute(&self, tx: Tx<2>) -> Tx<4> {
//         todo!()
//     }
// }
// // impl AppC<2, 4> for FirstInstructionC {}
// impl<const SIZE_IN: usize, const SIZE_OUT: usize> Application<SIZE_IN, SIZE_OUT> for FirstInstructionC {
//     fn execute(&self, tx: Tx<SIZE_IN>) -> Tx<SIZE_OUT> {
//         todo!()
//     }
// }
//
// struct SecondInstructionC;
// impl Application<10, 20> for SecondInstructionC {
//     fn execute(&self, tx: Tx<10>) -> Tx<20> {
//         todo!()
//     }
// }
// impl AppC<10, 20> for SecondInstructionC {}
//endregion