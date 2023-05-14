use std::cell::RefCell;
use std::collections::HashMap;
use std::hash::BuildHasherDefault;
use std::mem;
use std::ops::Range;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH, Instant};

use ahash::{AHasher};
use crossbeam::channel::{Receiver, Sender, unbounded};
use futures::SinkExt;
use itertools::Itertools;
use rayon::prelude::*;
use strum::IntoEnumIterator;
use thincollections::thin_map::ThinMap;
use tokio::time::{Duration};

use crate::contract::{Access, AccessPattern, AccessType, AtomicFunction, FunctionResult, MAX_NB_ADDRESSES, StaticAddress, Transaction};
use crate::contract::FunctionResult::Another;
use crate::d_hash_map::DHashMap;
use crate::key_value::KeyValueOperation;
use crate::vm::Executor;
use crate::vm_utils::{AddressSet, Scheduling, VmResult, VmStorage, VmUtils};
use crate::wip::Word;

#[derive(Debug)]
pub struct ParallelVmCollect {
    pub vm: ParallelVM
}

impl ParallelVmCollect {
    pub fn new(storage_size: usize, nb_schedulers: usize, nb_workers: usize) -> anyhow::Result<Self> {
        let mut vm = ParallelVM::new(storage_size, nb_schedulers, nb_workers)?;
        // vm.scheduler_chunk_size = |backlog_size: usize, nb_schedulers: usize| {
        //     65536/nb_schedulers + 1
        // };
        return Ok(Self{vm});
    }

    #[inline]
    pub fn execute<const A: usize, const P: usize>(&mut self, mut batch: Vec<Transaction<A, P>>) -> anyhow::Result<VmResult<A, P>> {

        let res = self.vm.execute_variant_6(batch);
        // DHashMap::println::<P>(&self.vm.storage.content);
        // DHashMap::print_bucket_sizes::<P>(&self.vm.storage.content);
        res
    }

    pub fn set_storage(&mut self, value: Word) {
        self.vm.storage.set_storage(value);
    }

    pub fn init_storage(&mut self, init: Box<dyn Fn(&mut Vec<Word>)>) {
        self.vm.init_storage(init);
    }

    pub fn terminate(&mut self) -> (Vec<Duration>, Vec<Duration>) {
        self.vm.terminate()
    }
}

#[derive(Debug)]
pub struct ParallelVmImmediate {
    pub vm: ParallelVM
}

impl ParallelVmImmediate {
    pub fn new(storage_size: usize, nb_schedulers: usize, nb_workers: usize) -> anyhow::Result<Self> {
        let mut vm = ParallelVM::new(storage_size, nb_schedulers, nb_workers)?;
        // vm.scheduler_chunk_size = |backlog_size: usize, nb_schedulers: usize| {
        //     backlog_size/nb_schedulers + 1
        // };
        return Ok(Self{vm});
    }

    #[inline]
    pub fn execute<const A: usize, const P: usize>(&mut self, mut batch: Vec<Transaction<A, P>>) -> anyhow::Result<VmResult<A, P>> {
        self.vm.execute_variant_7(batch)
    }

    pub fn set_storage(&mut self, value: Word) {
        self.vm.storage.set_storage(value);
    }

    pub fn init_storage(&mut self, init: Box<dyn Fn(&mut Vec<Word>)>) {
        self.vm.init_storage(init);
    }

    pub fn terminate(&mut self) -> (Vec<Duration>, Vec<Duration>) {
        self.vm.terminate()
    }
}

// #[derive(Debug)]
// pub struct ParallelVmDynamic {
//     pub vm: ParallelVM
// }
//
// impl ParallelVmDynamic {
//     /* TODO Add variant with more control on dispatch: executor pool sends work to schedulers one by one
//     so it can choose how to split the work among them
//  */
//     pub fn new(storage_size: usize, nb_schedulers: usize, nb_workers: usize) -> anyhow::Result<Self> {
//         let mut vm = ParallelVM::new(storage_size, nb_schedulers, nb_workers)?;
//         // vm.scheduler_chunk_size = |backlog_size: usize, nb_schedulers: usize| {
//         //     backlog_size/nb_schedulers + 1
//         // };
//         return Ok(Self{vm});
//     }
//
//     pub fn execute(&mut self, mut batch: Vec<Transaction>) -> anyhow::Result<(Duration, Duration)> {
//         self.vm.execute_variant_7(batch)
//     }
// }

#[derive(Debug)]
pub struct ParallelVM {
    pub storage: VmStorage,
    functions: Vec<AtomicFunction>,
    nb_schedulers: usize,
    scheduler_chunk_size: fn(usize, usize)->usize,
    nb_workers: usize,
    executor_chunk_size: fn(usize, usize)->usize,
    execution_latencies: Vec<Duration>,
    scheduling_latencies: Vec<Duration>,
}

impl ParallelVM {
    pub fn new(storage_size: usize, nb_schedulers: usize, nb_workers: usize) -> anyhow::Result<Self> {
        let storage = VmStorage::new(storage_size);
        let functions = AtomicFunction::all();

        // TODO Try to reuse channels/vectors
        // let (tx_nb_rounds, rx_nb_rounds) = unbounded();
        // let (sets, (scheduling_outputs, execution_inputs)): (Vec<_>, (Vec<_>, Vec<_>)) = (0..nb_schedulers).map(|_| {
        //     (AddressSet::with_capacity(MAX_NB_ADDRESSES * 65536), unbounded())
        // }).unzip();
        //
        // let (new_tx, scheduling_input) = unbounded();
        // let (tx_batch_done, rx_batch_done) = unbounded();

        let scheduler_chunk_size = |backlog_size: usize, nb_schedulers: usize| {
            (backlog_size/nb_schedulers) + 1
            // 65536/self.nb_schedulers + 1
            // backlog_size/self.nb_schedulers + 1
            // if backlog_size >= self.nb_schedulers {
            //     backlog_size/self.nb_schedulers + 1
            // } else {
            //     backlog_size
            // }
        };

        let executor_chunk_size = |round_size: usize, nb_executors: usize| {
            (round_size/nb_executors) + 1
            // // (65536/self.nb_schedulers+1)/self.nb_executors + 1
            // round_size/self.nb_executors + 1
            // if round_size >= self.nb_executors {
            //     round_size/self.nb_executors + 1
            // } else {
            //     round_size
            // }
        };

        let scheduling_latencies = Vec::with_capacity(65536);
        let execution_latencies = Vec::with_capacity(65536);

        let vm = Self{
            storage,
            functions,
            nb_schedulers,
            scheduler_chunk_size,
            nb_workers,
            executor_chunk_size,
            scheduling_latencies,
            execution_latencies,
        };
        return Ok(vm);
    }

    pub fn init_storage(&mut self, init: Box<dyn Fn(&mut Vec<Word>)>) {
        init(&mut self.storage.content)
    }

    pub fn terminate(&mut self) -> (Vec<Duration>, Vec<Duration>) {
        (self.scheduling_latencies.clone(), self.execution_latencies.clone())
    }

    #[inline]
    fn get_executor_chunk_size(&self, round_size: usize) -> usize {
        (self.executor_chunk_size)(round_size, self.nb_workers)
    }

    #[inline]
    fn get_scheduler_chunk_size(&self, backlog_size: usize) -> usize {
        (self.scheduler_chunk_size)(backlog_size, self.nb_schedulers)
    }

    #[inline]
    fn get_address_set_capacity(&self, chunk_size: usize) -> usize {
        // max(MAX_NB_ADDRESSES * chunk_size, MAX_NB_ADDRESSES * 65536 / (self.nb_schedulers /2))
        // MAX_NB_ADDRESSES * 65536
        MAX_NB_ADDRESSES * chunk_size // TODO
    }
    pub fn schedule_chunk<const A: usize, const P: usize>(&self, mut chunk: Vec<Transaction<A, P>>) -> (Vec<Transaction<A, P>>, Vec<Transaction<A, P>>) {
        // self.schedule_chunk_old(chunk)
        self.schedule_chunk_new(chunk)
    }

    pub fn schedule_chunk_old<const A: usize, const P: usize>(&self, mut chunk: Vec<Transaction<A, P>>) -> (Vec<Transaction<A, P>>, Vec<Transaction<A, P>>) {
        // let a = Instant::now();
        let mut scheduled = Vec::with_capacity(chunk.len());
        let mut postponed = Vec::with_capacity(chunk.len());

        let mut working_set = AddressSet::with_capacity(
            self.get_address_set_capacity(chunk.len())
        );

        Scheduling::schedule_chunk_old(chunk, scheduled, postponed, working_set,
                                       // a
        )
    }


    pub fn schedule_chunk_new<const A: usize, const P: usize>(&self, mut chunk: Vec<Transaction<A, P>>) -> (Vec<Transaction<A, P>>, Vec<Transaction<A, P>>) {
        // let a = Instant::now();
        let mut scheduled = Vec::with_capacity(chunk.len());
        let mut postponed = Vec::with_capacity(chunk.len());

        let addresses_per_tx = A;
        // let addresses_per_tx = 10;
        let mut address_map_capacity = addresses_per_tx * chunk.len();
        address_map_capacity *= 2;
        type Map = HashMap<StaticAddress, AccessType, BuildHasherDefault<AHasher>>;
        let mut address_map: Map = HashMap::with_capacity_and_hasher(address_map_capacity, BuildHasherDefault::default());

        Scheduling::schedule_chunk_new(&mut chunk, &mut scheduled, &mut postponed, &mut address_map,
                                       // a
        );

        (scheduled, postponed)
    }

    fn execute_tx<const A: usize, const P: usize>(&self, tx: &Transaction<A, P>) -> Option<Transaction<A, P>> {
        // execute the transaction and optionally generate a new tx
        // let function = self.functions.get(tx.function as usize).unwrap();
        let function = tx.function;
        match unsafe { function.execute(tx.clone(), self.storage.get_shared()) } {
            Another(generated_tx) => Some(generated_tx),
            _ => None,
        }
    }

    fn execute_chunk<const A: usize, const P: usize>(&self, mut worker_backlog: &[Transaction<A, P>]) -> Vec<Transaction<A, P>> {

        let res: Vec<_> = worker_backlog
            // .drain(..worker_backlog.len())
            .into_iter()
            .flat_map(|tx| self.execute_tx(tx))
            .collect();
        res
    }

    // TODO replace flatmap with map + for loop?
    pub fn execute_round<const A: usize, const P: usize>(&self, mut round: Vec<Transaction<A, P>>) -> Vec<Transaction<A, P>> {
        let chunk_size = self.get_executor_chunk_size(round.len());
        // let mut result = Vec::with_capacity(round.len());
        // result.par_extend(round
        //     .par_chunks(chunk_size)
        //     .enumerate()
        //     .flat_map(
        //         |(worker_index, worker_backlog)|
        //             self.execute_chunk(worker_backlog)
        //     ));
        // result
        round
            .par_chunks(chunk_size)
            .enumerate()
            .flat_map(
                |(worker_index, worker_backlog)| {
                    VmUtils::timestamp(format!("Executor {} starts executing", worker_index).as_str());
                    let res = self.execute_chunk(worker_backlog);
                    VmUtils::timestamp(format!("Executor {} finished executing", worker_index).as_str());
                    res
                }
            ).collect()

        // TODO Try to let rayon optimise execution
        // round.par_iter().flat_map(|tx| self.execute_tx(tx)).collect()
    }

    pub fn execute_variant_6<const A: usize, const P: usize>(&mut self, mut batch: Vec<Transaction<A, P>>) -> anyhow::Result<VmResult<A, P>> {

        let (mut send_generated_tx, receive_generated_tx) = unbounded();
        let mut scheduler_outputs = Vec::with_capacity(self.nb_schedulers);
        let mut worker_pool_inputs = Vec::with_capacity(self.nb_schedulers);
        for _ in 0..self.nb_schedulers {
            let (scheduler_out, worker_pool_in) = unbounded();
            scheduler_outputs.push(scheduler_out);
            worker_pool_inputs.push(worker_pool_in);
        }

        let batch_size = batch.len();
        send_generated_tx.send(batch)?;

        // TODO duplicated code 1
        let scheduling = |new_backlog: Receiver<Vec<Transaction<A, P>>>, mut outputs: Vec<Sender<Vec<Transaction<A, P>>>>| {
            VmUtils::timestamp("Scheduling closure starts");
            // let a = Instant::now();
            // let mut waited = Duration::from_secs(0);
            let mut duration = Duration::from_secs(0);
            // let mut b = Instant::now();
            VmUtils::timestamp("Scheduling waits for first backlog");
            while let Ok(mut backlog) = new_backlog.recv() {
                VmUtils::timestamp("Scheduling receives backlog");
                let scheduling_start = Instant::now();
                // waited += b.elapsed();
                while !backlog.is_empty() {
                    VmUtils::timestamp("Start scheduling backlog");
                    let chunk_size = self.get_scheduler_chunk_size(backlog.len());
                    let mut chunks = backlog.par_drain(..backlog.len()).chunks(chunk_size);
                    if chunks.len() > outputs.len() { panic!("Not enough output channels!"); }

                    let schedulers_postponed: Vec<_> = chunks
                        .zip(outputs.par_iter())
                        .enumerate()
                        .map(|(scheduler_index, (chunk, output))| {
                            VmUtils::timestamp(format!("Scheduler {} starts scheduling", scheduler_index).as_str());
                            let (scheduled, postponed) = self.schedule_chunk(chunk);
                            if scheduled.is_empty() { panic!("Scheduler produced an empty schedule"); }
                            VmUtils::timestamp(format!("Scheduler {} sends schedule", scheduler_index).as_str());
                            if let Err(e) = output.send(scheduled) {
                                panic!("Failed to send schedule: {:?}", e.into_inner());
                            }
                            postponed
                        }).collect();
                    // let mut schedulers_postponed = Vec::with_capacity(outputs.len());
                    // schedulers_postponed.par_extend(chunks
                    //     .zip(outputs.par_iter())
                    //     .enumerate()
                    //     .map(|(scheduler_index, (chunk, output))| {
                    //         let (scheduled, postponed) = self.schedule_chunk(chunk);
                    //         if scheduled.is_empty() { panic!("Scheduler produced an empty schedule"); }
                    //         if let Err(e) = output.send(scheduled) {
                    //             panic!("Failed to send schedule: {:?}", e.into_inner());
                    //         }
                    //         postponed
                    //     }));
                    VmUtils::timestamp("Done scheduling, marking end of output");
                    // Notify the executor pool of the first scheduler without an output
                    // This ensures that the executor pool does not wait in schedulers that did not receive
                    // an input (and therefore will not output any result through their channel)
                    if schedulers_postponed.len() < self.nb_schedulers {
                        if let Err(e) = outputs[schedulers_postponed.len()].send(vec!()) {
                            panic!("Failed to send empty schedule: {}", schedulers_postponed.len());
                        }
                    }
                    VmUtils::timestamp("Done scheduling, extending backlog");
                    for mut postponed in schedulers_postponed {
                        backlog.append(&mut postponed);
                    }
                    VmUtils::timestamp(format!("Done scheduling, backlog size: {}", backlog.len()).as_str());
                }

                VmUtils::timestamp("Scheduling waits for next backlog");
                duration += scheduling_start.elapsed();
                // b = Instant::now();
            }
            // VmUtils::timestamp("End of scheduling closure");
            // eprintln!("scheduling closure took {:?}", a.elapsed());
            // eprintln!("\tincl. {:?} waiting for input from executor", waited);
            // VmUtils::took("scheduling closure", a.elapsed());
            // VmUtils::took("\t waiting for input from executor", waited);
            anyhow::Ok(duration)
        };

        let execution = |inputs: Vec<Receiver<Vec<Transaction<A, P>>>>, mut scheduling_pool: Sender<Vec<Transaction<A, P>>>| {
                VmUtils::timestamp("Execution closure starts");
                // let a = Instant::now();
                // let mut waited = Duration::from_secs(0);
                // TODO return results of transactions
                let mut duration = Duration::from_secs(0);
                let mut executed = 0;
                let mut execution_rounds = 0;
                VmUtils::timestamp("Executor pool start outer loop");
                'outer_loop: loop {
                    let mut generated_tx = vec!();
                    VmUtils::timestamp("Executor pool start round robin");
                    'round_robin: for (scheduler_index, input) in inputs.iter().enumerate() {
                        // let b = Instant::now();
                        VmUtils::timestamp(format!("Executor pool waits for input from scheduler {}", scheduler_index).as_str());
                        if let Ok(round) = input.recv() {
                            VmUtils::timestamp(format!("Executor pool receive schedule from scheduler {}", scheduler_index).as_str());
                            // waited += b.elapsed();
                            let execution_start = Instant::now();
                            if round.is_empty() {
                                VmUtils::timestamp("Executor pool breaks round robin");
                                break 'round_robin;
                            }
                            execution_rounds += 1;

                            executed += round.len();
                            VmUtils::timestamp("Executor pool: execution start");
                            let mut new_tx = self.execute_round(round);
                            VmUtils::timestamp("Executor pool: execution end");

                            executed -= new_tx.len();

                            generated_tx.append(&mut new_tx);
                            duration += execution_start.elapsed();
                            VmUtils::timestamp("Executor pool done with this schedule");
                        } else {
                            VmUtils::timestamp("Executor pool failed to receive schedule");
                        }
                    }

                    VmUtils::timestamp("Executor pool round robin ends, sending new tx or exiting");
                    if generated_tx.is_empty() {
                        if executed == batch_size {
                            break;
                        }
                    } else if let Err(e) = scheduling_pool.send(generated_tx) {
                        panic!("Failed to send generated tx: {:?}", e);
                    }
                }
                // VmUtils::timestamp("End of execution closure");
                // VmUtils::took("execution closure", a.elapsed());
                // VmUtils::took("\t waiting for input from scheduler", waited);
                // VmUtils::nb_rounds(execution_rounds);
                // eprintln!("execution closure took {:?}", a.elapsed());
                // eprintln!("\tincl. {:?} waiting for input from scheduler", waited);
                anyhow::Ok(duration)
            };

        // let a = Instant::now();
        VmUtils::timestamp("rayon start");
        let (scheduling_res, execution_res) = rayon::join(
            || {
                VmUtils::timestamp("first closure starts");
                scheduling(receive_generated_tx, scheduler_outputs)
            },
            || {
                VmUtils::timestamp("second closure starts");
                execution(worker_pool_inputs, send_generated_tx)
            },
        );
        VmUtils::timestamp("rayon end");
        // VmUtils::took("rayon::join", a.elapsed());

        // let scheduling_duration = scheduling_res.unwrap();
        // let execution_duration = execution_res.unwrap();
        Ok(VmResult::new(vec!(), scheduling_res.ok(), execution_res.ok()))

        // self.scheduling_latencies.push(scheduling_res.unwrap());
        // self.execution_latencies.push(execution_res.unwrap());
        //
        // Ok(VmResult::new(vec!(), None, None))
    }

    pub fn execute_variant_7<const A: usize, const P: usize>(&mut self, mut batch: Vec<Transaction<A, P>>) -> anyhow::Result<VmResult<A, P>> {

        let mut scheduler_outputs = Vec::with_capacity(self.nb_schedulers);
        let mut worker_pool_inputs = Vec::with_capacity(self.nb_schedulers);
        for _ in 0..self.nb_schedulers {
            let (scheduler_out, worker_pool_in) = unbounded();
            scheduler_outputs.push(scheduler_out);
            worker_pool_inputs.push(worker_pool_in);
        }
        let (send_generated_tx, receive_generated_tx) = unbounded();

        let batch_size = batch.len();
        send_generated_tx.send(batch)?;

        // TODO duplicated code 1
        let scheduling = |new_backlog: Receiver<Vec<Transaction<A, P>>>, mut outputs: Vec<Sender<Vec<Transaction<A, P>>>>| {
            // let a = Instant::now();
            // let mut waited = Duration::from_secs(0);
            let mut duration = Duration::from_secs(0);
            // let mut b = Instant::now();
            while let Ok(mut backlog) = new_backlog.recv() {
                // waited += b.elapsed();
                let scheduling_start = Instant::now();

                while !backlog.is_empty() {
                    let chunk_size = self.get_scheduler_chunk_size(backlog.len());
                    let mut chunks = backlog.par_drain(..backlog.len()).chunks(chunk_size);
                    if chunks.len() > outputs.len() { panic!("Not enough output channels!"); }

                    let schedulers_postponed: Vec<_> = chunks
                        .zip(outputs.par_iter())
                        .enumerate()
                        .map(|(scheduler_index, (chunk, output))| {
                            let (scheduled, postponed) = self.schedule_chunk(chunk);
                            if scheduled.is_empty() { panic!("Scheduler produced an empty schedule"); }
                            if let Err(e) = output.send(scheduled) {
                                panic!("Failed to send schedule: {:?}", e.into_inner());
                            }
                            postponed
                        }).collect();

                    // Notify the executor pool of the first scheduler without an output
                    // This ensures that the executor pool does not wait in schedulers that did not receive
                    // an input (and therefore will not output any result through their channel)
                    if schedulers_postponed.len() < self.nb_schedulers {
                        if let Err(e) = outputs[schedulers_postponed.len()].send(vec!()) {
                            panic!("Failed to send empty schedule: {}", schedulers_postponed.len());
                        }
                    }

                    for mut postponed in schedulers_postponed {
                        backlog.append(&mut postponed);
                    }
                }

                duration += scheduling_start.elapsed();
                // b = Instant::now();
            }
            // eprintln!("scheduling closure took {:?}", a.elapsed());
            // eprintln!("\tincl. {:?} waiting for input from executor", waited);
            // VmUtils::took("scheduling closure", a.elapsed());
            // VmUtils::took("\t waiting for input from executor", waited);
            anyhow::Ok(duration)
        };

        // TODO return the channels so that they are not dropped
        let execution = |inputs: Vec<Receiver<Vec<Transaction<A, P>>>>, mut scheduling_pool: Sender<Vec<Transaction<A, P>>>| {
                // let a = Instant::now();
                // let mut waited = Duration::from_secs(0);
                let mut duration = Duration::from_secs(0);
                let mut executed = 0;
            let mut execution_rounds = 0;
                'outer_loop: loop {
                    'round_robin: for input in inputs.iter() {
                        // let b = Instant::now();
                        if let Ok(round) = input.recv() {
                            // waited += b.elapsed();
                            let execution_start = Instant::now();
                            if round.is_empty() {
                                break 'round_robin;
                            }

                            executed += round.len();
                            let mut new_tx = self.execute_round(round);
                            executed -= new_tx.len();
                            execution_rounds += 1;
                            if !new_tx.is_empty() {
                                if let Err(e) = scheduling_pool.send(new_tx) {
                                    panic!("Failed to send generated tx: {:?}", e);
                                }
                            }
                            duration += execution_start.elapsed();
                        }
                    }

                    if executed == batch_size {
                        break;
                    }
                }
                // eprintln!("execution closure took {:?}", a.elapsed());
                // eprintln!("\tincl. {:?} waiting for input from scheduler", waited);
                // VmUtils::took("execution closure", a.elapsed());
                // VmUtils::took("\t waiting for input from scheduler", waited);
                // VmUtils::nb_rounds(execution_rounds);
                anyhow::Ok(duration)
            };

        // let a = Instant::now();
        let (scheduling_res, execution_res) = rayon::join(
            || scheduling(receive_generated_tx, scheduler_outputs),
            || execution(worker_pool_inputs, send_generated_tx),
        );
        // eprintln!("rayon::join took {:?}", a.elapsed());
        // VmUtils::took("rayon::join", a.elapsed());

        // let scheduling_duration = scheduling_res.unwrap();
        // let execution_duration = execution_res.unwrap();
        Ok(VmResult::new(vec!(), scheduling_res.ok(), execution_res.ok()))

        // self.scheduling_latencies.push(scheduling_res.unwrap());
        // self.execution_latencies.push(execution_res.unwrap());
        //
        // Ok(VmResult::new(vec!(), None, None))
    }
}

// trait BatchChunk<const A: usize, const P: usize> {
//     fn get(&self, index: usize) -> Option<Transaction<A, P>>;
// }
pub enum Job<const A: usize, const P: usize> {
    Schedule(Vec<Transaction<A, P>>),
    Execute(Vec<Transaction<A, P>>),
}
pub enum Round<const A: usize, const P: usize> {
    ReadOnly(Vec<Transaction<A, P>>),
    Exclusive(Vec<Transaction<A, P>>),
    Parallel(Vec<Transaction<A, P>>),
}
struct Foo {
    pub ro: Vec<usize>,
    pub exclusive: Vec<usize>,
    pub parallel: Vec<Range<usize>>,
}
impl Foo {
    pub fn new(chunk_size: usize) -> Self {
        Self {
            ro: Vec::with_capacity(chunk_size),
            exclusive: Vec::with_capacity(chunk_size),
            parallel: Vec::with_capacity(chunk_size),
        }
    }
}

struct VmWorker<const A: usize, const P: usize> {
    // // scheduling
    // pub postponed: Vec<Transaction<A, P>>,
    pub address_map: HashMap<StaticAddress, AccessType, BuildHasherDefault<AHasher>>,
    // pub scheduling_job_in: Receiver<Vec<Transaction<A, P>>>,
    //
    // // execution
    // pub execution_job_in: Receiver<Vec<Transaction<A, P>>>,
    // pub signal_execution_end: Sender<()>,
    pub input: Receiver<Job<A, P>>,

    // Option 1: store indices then move the transactions in the result vector
    //      + move tx only once (not really, you moved them to read them...)
    //      - iterate twice
    // Option 2: store tx then move them to result vector
    //      - move tx twice
    //      + iterate only once
    //      + can drain the backlog
    // Option 3: move tx in separate result vectors and swap them at the end
    //      + move tx only once
    //      - need to pass vectors around to avoid allocations

    pub backlog: RefCell<Vec<usize>>,
    pub postponed: RefCell<Vec<usize>>,

    pub output: Sender<FunctionResult<A, P>>,
}
/*
impl<const A: usize, const P: usize> VmWorker<A, P> {
    pub fn run(mut self) {
        // loop {
            // Leader:
            //      Wait for scheduling backlog
            //      Send backlog to all threads
            // Schedule
            // If leader,
            //      if has schedule, send schedule to other threads with identity of next leader
            //      else we are done
            // else wait for schedule from leader
            // execute schedule
            // If leader, wait for notification from other threads
            // else notify leader
        // }
        loop {
            match self.input.recv() {
                Ok(Job::Schedule(backlog)) => {
                    // self.schedule(backlog);
                },
                Ok(Job::Execute(transactions)) => {
                    self.execute(transactions);
                },
                Err(e) => {
                    // Worker closes shop
                    return;
                }
            }
        }
    }

    fn schedule(&mut self, transactions: Arc<Vec<Transaction<A, P>>>, mut foo: Foo) {

        let mut backlog = self.backlog.borrow_mut();
        let mut address_map = self.address_map.borrow_mut();
        let mut postponed = self.postponed.borrow_mut();

        backlog.extend(0..transactions.len());

        while let Some(first_index) = backlog.first().map(|first| *first).clone() {
            self.schedule_once(first_index, &transactions, &mut foo, &mut address_map, &mut backlog, &mut postponed);
            // backlog is now empty
            // postponed might have some values
            backlog.borrow_mut().append(&mut postponed);
            address_map.clear();
        };

        // backlog is now empty
        // postponed is now empty

        // Send result (foo)
        // TODO
    }

    // TODO pass everything as arguments...
    fn schedule_once(&self,
                     first_index: usize,
                     transactions: &Arc<Vec<Transaction<A, P>>>,
                     foo: &mut Foo,
        address_map: &mut HashMap<StaticAddress, AccessType, BuildHasherDefault<AHasher>>,
        backlog: &mut Vec<usize>,
        postponed: &mut Vec<usize>,
    ) {
        let mut current_schedule = first_index..first_index;

        'backlog: for index in backlog.drain(0..) {
            let tx = transactions.get(index).unwrap();
            // TODO just return an array of Access::Done when there is no accesses instead of doing this weird thing
            let possible_accesses = tx.mixed_accesses();
            let accesses = possible_accesses.as_ref().map_or([].as_slice(), |inside| inside.as_slice());

            // let (possible_reads, possible_writes) = tx.accesses();
            // let reads = possible_reads.as_ref().map_or([].as_slice(), |inside| inside.as_slice());
            // let writes = possible_writes.as_ref().map_or([].as_slice(), |inside| inside.as_slice());

            // Tx without any memory accesses can always be scheduled ------------------------------
            // if reads.is_empty() && writes.is_empty() {
            if accesses.is_empty() {
                // Add this tx to the schedule
                current_schedule.end = index + 1;
                continue 'backlog;
            }

            'schedule_tx: for access in accesses {
                match access {
                    Access::Address(addr, access_type) => {
                        if (access_type.is_read() && !self.can_read(*addr, address_map)) || !self.can_write(*addr, address_map) {
                            // Postpone this tx
                            postponed.push(index);

                            // Commit the current schedule
                            if !current_schedule.is_empty() {
                                foo.parallel.push(current_schedule.clone());
                            }

                            // Start a new schedule
                            // self.address_map.clear();
                            current_schedule = (index+1)..(index+1);
                            continue 'backlog;
                        }
                    },
                    Access::Range(from, to, access_type) => {
                        assert!(from < to);
                        'range: for addr in (*from)..(*to) {
                            if (access_type.is_read() && !self.can_read(addr, address_map)) || !self.can_write(addr, address_map) {
                                // Postpone this tx
                                postponed.push(index);

                                // Commit the current schedule
                                if !current_schedule.is_empty() {
                                    foo.parallel.push(current_schedule.clone());
                                }

                                // Start a new schedule
                                // self.address_map.clear();
                                current_schedule = (index+1)..(index+1);
                                continue 'backlog;
                            }
                        }
                    },
                    Access::All(access_type) => {
                        // Commit the current schedule
                        if !current_schedule.is_empty() {
                            foo.parallel.push(current_schedule.clone());
                        }

                        if access_type.is_read() {
                            foo.ro.push(index);
                        } else {
                            foo.exclusive.push(index);
                        }

                        // Start a new schedule
                        // self.address_map.clear();
                        current_schedule = (index+1)..(index+1);
                        continue 'backlog;
                    },
                    Access::Done => {
                        break 'schedule_tx;
                    }
                }
            }

            // Transaction can be executed in parallel, add it to the schedule
            current_schedule.end = index + 1;
        }

        // Commit the current schedule
        if !current_schedule.is_empty() {
            foo.parallel.push(current_schedule);
        }
    }

    fn reset(&mut self) {

    }

    fn can_read(&self, addr: StaticAddress, address_map: &mut HashMap<StaticAddress, AccessType, BuildHasherDefault<AHasher>>) -> bool {
        let entry = address_map.entry(addr).or_insert(AccessType::Read);
        *entry == AccessType::Read
    }

    fn can_write(&self, addr: StaticAddress, address_map: &mut HashMap<StaticAddress, AccessType, BuildHasherDefault<AHasher>>) -> bool {
        let entry = address_map.entry(addr).or_insert(AccessType::TryWrite);
        if *entry == AccessType::TryWrite {
            *entry = AccessType::Write;
            true
        } else {
            false
        }
    }

    fn execute(&mut self, transactions: Vec<Transaction<A, P>>) {

    }
}
*/
//
// #[derive(Debug)]
// pub struct WipOptimizedParallelVM {
//     pub storage: VmStorage,
//     functions: Vec<AtomicFunction>,
//
//     new_tx: Sender<Vec<Transaction>>,
//
//     nb_schedulers: usize,
//     scheduling_pool: SchedulingPool,
//     //
//     nb_workers: usize,
//     execution_pool: ExecutionPool,
// }
//
// impl WipOptimizedParallelVM {
//     pub fn new(storage_size: usize, nb_schedulers: usize, nb_workers: usize) -> anyhow::Result<Self> {
//
//         let storage = VmStorage::new(storage_size);
//         let functions = AtomicFunction::all();
//
//         let (tx_nb_rounds, rx_nb_rounds) = unbounded();
//         let (sets, (scheduling_outputs, execution_inputs)): (Vec<_>, (Vec<_>, Vec<_>)) = (0..nb_schedulers).map(|_| {
//             (AddressSet::with_capacity(MAX_NB_ADDRESSES * 65536), unbounded())
//         }).unzip();
//
//         let (new_tx, scheduling_input) = unbounded();
//         let (tx_batch_done, rx_batch_done) = unbounded();
//
//         let scheduling_pool = SchedulingPool::new(
//             nb_schedulers,
//             // sets,
//             scheduling_input,
//             tx_nb_rounds,
//             scheduling_outputs,
//             rx_batch_done
//         );
//
//         let execution_pool = ExecutionPool::new(
//             nb_workers,
//             rx_nb_rounds,
//             execution_inputs,
//             new_tx.clone(),
//             tx_batch_done
//         );
//
//         let vm = Self{
//             storage,
//             functions,
//             new_tx,
//             nb_schedulers,
//             scheduling_pool,
//             nb_workers,
//             execution_pool,
//         };
//         return Ok(vm);
//     }
//
//     fn schedule_chunk(mut chunk: Vec<Transaction>, set: &mut AddressSet) -> (Vec<Transaction>, Vec<Transaction>) {
//         let mut scheduled = Vec::with_capacity(chunk.len());
//         let mut postponed = Vec::with_capacity(chunk.len());
//
//         'outer: for tx in chunk {
//             for addr in tx.addresses.iter() {
//                 if !set.insert(*addr) {
//                     // Can't add tx to schedule
//                     postponed.push(tx);
//                     continue 'outer;
//                 }
//             }
//             scheduled.push(tx);
//         }
//
//         (scheduled, postponed)
//     }
//
//     fn schedule_chunk_bis(&self, mut chunk: Vec<Transaction>, set: &mut AddressSet) -> (Vec<Transaction>, Vec<Transaction>) {
//         let mut scheduled = Vec::with_capacity(chunk.len());
//         let mut postponed = Vec::with_capacity(chunk.len());
//
//         'outer: for tx in chunk {
//             for addr in tx.addresses.iter() {
//                 if !set.insert(*addr) {
//                     // Can't add tx to schedule
//                     postponed.push(tx);
//                     continue 'outer;
//                 }
//             }
//             scheduled.push(tx);
//         }
//
//         (scheduled, postponed)
//     }
//
//     #[inline]
//     fn get_scheduler_chunk_size(&self, backlog_size: usize) -> usize {
//         // 65536/self.nb_schedulers + 1
//         backlog_size/self.nb_schedulers + 1
//         // if backlog_size >= self.nb_schedulers {
//         //     backlog_size/self.nb_schedulers + 1
//         // } else {
//         //     backlog_size
//         // }
//     }
//
//     #[inline]
//     fn get_chunk_size(&self, round_size: usize) -> usize {
//         round_size/self.nb_workers + 1
//         // if round_size >= self.nb_workers {
//         //     round_size/self.nb_workers + 1
//         // } else {
//         //     round_size
//         // }
//     }
//
//     pub fn execute_variant_6(&mut self, mut batch: Vec<Transaction>) -> anyhow::Result<()> {
//
//         let batch_size = batch.len();
//
//         let mut scheduler_outputs = Vec::with_capacity(self.nb_schedulers);
//         let mut worker_pool_inputs = Vec::with_capacity(self.nb_schedulers);
//         for _ in 0..self.nb_schedulers {
//             let (scheduler_out, worker_pool_in) = unbounded();
//             scheduler_outputs.push(scheduler_out);
//             worker_pool_inputs.push(worker_pool_in);
//         }
//         let (send_generated_tx, receive_generated_tx) = unbounded();
//         // let (tx_nb_rounds, rx_nb_rounds) = unbounded();
//
//         send_generated_tx.send(batch)?;
//
//         let scheduling = |new_backlog: crossbeam::channel::Receiver<Vec<Transaction>>, mut outputs: Vec<crossbeam::channel::Sender<Vec<Transaction>>>| {
//             let mut duration = Duration::from_secs(0);
//             while let Ok(mut backlog) = new_backlog.recv() {
//                 let scheduling_start = Instant::now();
//
//                 while !backlog.is_empty() {
//                     let chunk_size = self.get_scheduler_chunk_size(backlog.len());
//                     let mut chunks = backlog.par_drain(..backlog.len()).chunks(chunk_size);
//                     // tx_nb_rounds.send(chunks.len());
//                     let res: Vec<_> = chunks
//                         .zip(outputs.par_iter_mut()).enumerate()
//                         .map(|(scheduler_index, (chunk, output))| {
//                             let mut set = AddressSet::with_capacity(2 * chunk.len());
//                             let (scheduled, postponed) = Self::schedule_chunk(chunk, &mut set);
//                             if let Err(e) = output.send(scheduled) {
//                                 panic!("Failed to send schedule");
//                             }
//                             postponed
//                         }).collect();
// // There is a bug here
//                     /* TODO move the job of splitting a batch into chunks to the executor pool
//                         Otherwise if there are less tx than there are schedulers, the executor pool
//                         will end up waiting on a channel that will never receive a msg.
//                         This is workaround to ensure the executor pool is never stuck
//                         */
//                     for i in new_backlog.len()..self.nb_schedulers {
//                         if let Err(e) = outputs[i].send(vec!()) {
//                             panic!("Failed to send empty schedule");
//                         }
//                     }
//
//                     for mut postponed in res {
//                         backlog.append(&mut postponed);
//                     }
//                 }
//
//                 duration += scheduling_start.elapsed();
//             }
//             anyhow::Ok(duration)
//         };
//
//         let execution =
//             |inputs: Vec<crossbeam::channel::Receiver<Vec<Transaction>>>, mut scheduling_pool: crossbeam::channel::Sender<Vec<Transaction>>| {
//                 let mut duration = Duration::from_secs(0);
//                 let mut completed = 0;
//                 loop {
//                     let mut generated_tx = vec!();
//                     'inner_loop: for input in inputs.iter() {
//                         if let Ok(round) = input.recv() {
//                             let execution_start = Instant::now();
//                             if round.is_empty() {
//                                 continue 'inner_loop;
//                             }
//                             let mut new_tx = self.execute_round(round);
//                             generated_tx.append(&mut new_tx);
//                             duration += execution_start.elapsed();
//                         }
//                     }
//                     // let nb_rounds = match rx_nb_rounds.recv() {
//                     //     Ok(nb) => nb,
//                     //     Err(e) => panic!("Unable to receive nb of rounds")
//                     // };
//
//                     // let mut generated_tx = vec!();
//                     //
//                     // 'round_robin: for i in 0..self.nb_schedulers {
//                     //     // println!("Executors waiting on scheduler {}", i);
//                     //     if let Ok(round) = inputs[i].recv() {
//                     //         let execution_start = Instant::now();
//                     //
//                     //         let mut new_tx = self.execute_round(round);
//                     //
//                     //         generated_tx.append(&mut new_tx);
//                     //         duration += execution_start.elapsed();
//                     //     }
//                     // }
//
//                     if generated_tx.is_empty() {
//                         break;
//                     } else {
//                         scheduling_pool.send(generated_tx)?;
//                     }
//                 }
//                 anyhow::Ok(duration)
//             };
//
//         let (scheduling_res, execution_res) = rayon::join(
//             || scheduling(receive_generated_tx, scheduler_outputs),
//             // || self.closure(worker_pool_inputs, send_generated_tx),
//             || execution(worker_pool_inputs, send_generated_tx),
//         );
//
//         let scheduling_duration = scheduling_res.unwrap();
//         let execution_duration = execution_res.unwrap();
//
//         // println!("Variant 6:");
//         println!("\tScheduling (v0): {:?}", scheduling_duration);
//         println!("\tExecution (v2):  {:?}", execution_duration);
//         println!("\tSum: {:?}", scheduling_duration + execution_duration);
//
//         Ok(())
//     }
//
//     pub fn execute_variant_wtf(&mut self, mut batch: Vec<Transaction>) -> anyhow::Result<()> {
//
//         let batch_size = batch.len();
//
//         let mut scheduler_outputs = Vec::with_capacity(self.nb_schedulers);
//         let mut worker_pool_inputs = Vec::with_capacity(self.nb_schedulers);
//         for _ in 0..self.nb_schedulers {
//             let (scheduler_out, worker_pool_in) = unbounded();
//             scheduler_outputs.push(scheduler_out);
//             worker_pool_inputs.push(worker_pool_in);
//         }
//         let (send_generated_tx, receive_generated_tx) = unbounded();
//         let (tx_nb_rounds, rx_nb_rounds) = unbounded();
//
//         send_generated_tx.send(batch)?;
//
//         let scheduling = |new_backlog: crossbeam::channel::Receiver<Vec<Transaction>>, mut outputs: Vec<crossbeam::channel::Sender<Vec<Transaction>>>| {
//             let mut duration = Duration::from_secs(0);
//             while let Ok(mut backlog) = new_backlog.recv() {
//                 let scheduling_start = Instant::now();
//
//                 while !backlog.is_empty() {
//                     let chunk_size = self.get_scheduler_chunk_size(backlog.len());
//                     let mut chunks = backlog.par_drain(..backlog.len()).chunks(chunk_size);
//                     tx_nb_rounds.send(chunks.len());
//                     let res: Vec<_> = chunks
//                         .zip(outputs.par_iter_mut()).enumerate()
//                         .map(|(scheduler_index, (chunk, output))| {
//                             let mut set = AddressSet::with_capacity(2 * chunk_size);
//                             let (scheduled, postponed) = self.schedule_chunk_bis(chunk, &mut set);
//                             if let Err(e) = output.send(scheduled) {
//                                 panic!("Failed to send schedule");
//                             }
//                             postponed
//                         }).collect();
// // There is a bug here
//                     /* TODO move the job of splitting a batch into chunks to the executor pool
//                         Otherwise if there are less tx than there are schedulers, the executor pool
//                         will end up waiting on a channel that will never receive a msg.
//                         This is workaround to ensure the executor pool is never stuck
//                         */
//                     for i in new_backlog.len()..self.nb_schedulers {
//                         if let Err(e) = outputs[i].send(vec!()) {
//                             panic!("Failed to send empty schedule");
//                         }
//                     }
//
//                     for mut postponed in res {
//                         backlog.append(&mut postponed);
//                     }
//                 }
//
//                 duration += scheduling_start.elapsed();
//             }
//             anyhow::Ok(duration)
//         };
//
//         let execution =
//             |inputs: Vec<crossbeam::channel::Receiver<Vec<Transaction>>>, mut scheduling_pool: crossbeam::channel::Sender<Vec<Transaction>>| {
//                 let mut duration = Duration::from_secs(0);
//                 let mut completed = 0;
//                 loop {
//                     let mut generated_tx = vec!();
//                     'inner_loop: for input in inputs.iter() {
//                         if let Ok(round) = input.recv() {
//                             let execution_start = Instant::now();
//                             if round.is_empty() {
//                                 continue 'inner_loop;
//                             }
//                             let mut new_tx = self.execute_round( round);
//                             generated_tx.append(&mut new_tx);
//                             duration += execution_start.elapsed();
//                         }
//                     }
//
//                     if generated_tx.is_empty() {
//                         break;
//                     } else {
//                         scheduling_pool.send(generated_tx)?;
//                     }
//                 }
//                 anyhow::Ok(duration)
//             };
//
//         let (scheduling_res, execution_res) = rayon::join(
//             || scheduling(receive_generated_tx, scheduler_outputs),
//             // || self.closure(worker_pool_inputs, send_generated_tx),
//             || execution(worker_pool_inputs, send_generated_tx),
//         );
//
//         let scheduling_duration = scheduling_res.unwrap();
//         let execution_duration = execution_res.unwrap();
//
//         // println!("Variant 6:");
//         println!("\tScheduling (v0): {:?}", scheduling_duration);
//         println!("\tExecution (v???):  {:?}", execution_duration);
//         println!("\tSum: {:?}", scheduling_duration + execution_duration);
//
//         Ok(())
//     }
//
//     pub fn execute(&mut self, mut batch: Vec<Transaction>) -> anyhow::Result<()> {
//
//         println!("Sending new tx to schedulers");
//         self.new_tx.send(batch)?;
//
//         println!("Starting scheduling and execution");
//         let (scheduling_res, execution_res) = rayon::join(
//             || self.scheduling_pool.schedule(),
//             || self.execution_pool.execute(&self.functions, self.storage.get_shared()),
//         );
//
//         let scheduling_duration = scheduling_res.unwrap();
//         let execution_duration = execution_res.unwrap();
//
//         println!("\tScheduling: {:?}", scheduling_duration);
//         println!("\tExecution (v2):  {:?}", execution_duration);
//         println!("\tSum: {:?}", scheduling_duration + execution_duration);
//
//         Ok(())
//     }
//     //
//     // fn schedule_chunk(mut chunk: Vec<Transaction>, set: &mut AddressSet) -> (Vec<Transaction>, Vec<Transaction>) {
//     //     let mut scheduled = Vec::with_capacity(chunk.len());
//     //     let mut postponed = Vec::with_capacity(chunk.len());
//     //
//     //     'outer: for tx in chunk {
//     //         for addr in tx.addresses.iter() {
//     //             if !set.insert(*addr) {
//     //                 // Can't add tx to schedule
//     //                 postponed.push(tx);
//     //                 continue 'outer;
//     //             }
//     //         }
//     //         scheduled.push(tx);
//     //     }
//     //
//     //     (scheduled, postponed)
//     // }
//     //
//     // fn scheduling(scheduling_pool: &mut SchedulingPool) -> anyhow::Result<Duration> {
//     //     let mut duration = Duration::from_secs(0);
//     //
//     //     let mut sets: Vec<_> = (0..scheduling_pool.nb_schedulers).map(|_| AddressSet::with_capacity(MAX_NB_ADDRESSES * 65536)).collect();
//     //
//     //     while let Ok(mut backlog) = scheduling_pool.input.recv() {
//     //         let scheduling_start = Instant::now();
//     //
//     //         while !backlog.is_empty() {
//     //             let chunk_size = scheduling_pool.get_chunk_size(backlog.len());
//     //             let mut chunks = backlog.par_drain(..backlog.len()).chunks(chunk_size);
//     //
//     //             if let Err(e) = scheduling_pool.tx_nb_rounds.send(chunks.len()) {
//     //                 panic!("Failed to send nb of rounds: {:?}", e);
//     //             }
//     //
//     //             let tmp: Vec<_> = chunks
//     //                 .zip(scheduling_pool.outputs.par_iter_mut())
//     //                 .zip(sets.par_iter_mut())
//     //                 .enumerate()
//     //                 .map(|(scheduler_index, ((chunk, output), set))| {
//     //
//     //                     let (scheduled, postponed) = Self::schedule_chunk(chunk, set);
//     //
//     //                     if let Err(e) = output.send(scheduled) {
//     //                         panic!("Failed to send schedule");
//     //                     }
//     //                     postponed
//     //                 }).collect();
//     //
//     //             // TODO There is a bug here?
//     //
//     //             for mut postponed in tmp {
//     //                 backlog.append(&mut postponed);
//     //             }
//     //
//     //             for set in sets.iter_mut() {
//     //                 set.clear();
//     //             }
//     //         }
//     //
//     //         duration += scheduling_start.elapsed();
//     //     }
//     //
//     //     anyhow::Ok(duration)
//     // }
//     //
//     fn execute_tx(&self, tx: &Transaction) -> Option<Transaction> {
//         // execute the transaction and optionally generate a new tx
//         // let function = self.functions.get(tx.function as usize).unwrap();
//         let function = tx.function;
//         match unsafe { function.execute(tx.clone(), self.storage.get_shared()) } {
//             Another(generated_tx) => Some(generated_tx),
//             _ => None,
//         }
//     }
//
//     fn execute_chunk(&self, mut worker_backlog: &[Transaction]) -> Vec<Transaction> {
//
//         // let w = Instant::now();
//         let res: Vec<_> = worker_backlog
//             // .drain(..worker_backlog.len())
//             .into_iter()
//             .flat_map(|tx| self.execute_tx(tx))
//             .collect();
//         // println!("\tSingle exec variant took {:?}", w.elapsed());
//         res
//     }
//
//     // TODO replace flatmap with map + for loop?
//     pub fn execute_round(&self, mut round: Vec<Transaction>) -> Vec<Transaction> {
//         let chunk_size = self.get_chunk_size(round.len());
//         round
//             // .into_par_iter()
//             // .par_drain(..round.len())
//             // .chunks(chunk_size)
//             .par_chunks(chunk_size)
//             .enumerate()
//             .flat_map(
//                 |(worker_index, worker_backlog)| self.execute_chunk(worker_backlog)
//             ).collect()
//     }
//     //
//     // fn execution(execution_pool: &ExecutionPool, functions: &Vec<AtomicFunction>, shared_storage: SharedStorage) -> anyhow::Result<Duration> {
//     //     let mut duration = Duration::from_secs(0);
//     //     loop {
//     //         let nb_rounds = match execution_pool.rx_nb_rounds.recv() {
//     //             Ok(nb) => nb,
//     //             Err(e) => panic!("Unable to receive nb of rounds: {:?}", e)
//     //         };
//     //
//     //         let mut generated_tx = vec!();
//     //
//     //         'round_robin: for i in 0..nb_rounds {
//     //             match execution_pool.inputs[i].recv() {
//     //                 Ok(round) => {
//     //                     let execution_start = Instant::now();
//     //
//     //                     let mut new_tx = Self::execute_round(round, functions, shared_storage);
//     //
//     //                     generated_tx.append(&mut new_tx);
//     //                     duration += execution_start.elapsed();
//     //                 },
//     //                 Err(e) => panic!("Unable to receive round: {:?}", e)
//     //             }
//     //         }
//     //
//     //         if generated_tx.is_empty() {
//     //             break;
//     //         } else {
//     //             // scheduling_pool.send(generated_tx)?;
//     //             execution_pool.new_tx.send(generated_tx)?;
//     //         }
//     //     }
//     //     anyhow::Ok(duration)
//     // }
// }
//
// //region Execution
// #[derive(Debug)]
// struct ExecutionPool {
//     nb_workers: usize,
//
//     rx_nb_rounds: Receiver<usize>,
//     inputs: Vec<Receiver<Vec<Transaction>>>,
//
//     new_tx: Sender<Vec<Transaction>>,
//     batch_done: Sender<()>
// }
//
// impl ExecutionPool {
//     fn new(nb_workers: usize,
//            rx_nb_rounds: Receiver<usize>,
//            inputs: Vec<Receiver<Vec<Transaction>>>,
//            new_tx: Sender<Vec<Transaction>>,
//            batch_done: Sender<()>) -> Self {
//
//         ExecutionPool{
//             nb_workers,
//             rx_nb_rounds,
//             inputs,
//             new_tx,
//             batch_done
//         }
//     }
//
//     fn execute(&mut self, functions: &Vec<AtomicFunction>, shared_storage: SharedStorage) -> anyhow::Result<Duration> {
//         let mut duration = Duration::from_secs(0);
//         loop {
//             // println!("Executors waiting to know the number of rounds");
//             let nb_rounds = match self.rx_nb_rounds.recv() {
//                 Ok(nb) => nb,
//                 Err(e) => panic!("Unable to receive nb of rounds")
//             };
//
//             let mut generated_tx = vec!();
//
//             'round_robin: for i in 0..nb_rounds {
//                 // println!("Executors waiting on scheduler {}", i);
//                 if let Ok(round) = self.inputs[i].recv() {
//                     let execution_start = Instant::now();
//
//                     let mut new_tx = self.execute_round(round, functions, shared_storage);
//
//                     generated_tx.append(&mut new_tx);
//                     duration += execution_start.elapsed();
//                 }
//             }
//
//             if generated_tx.is_empty() {
//                 // println!("No generated tx");
//                 self.batch_done.send(())?;
//                 break;
//             } else {
//                 // println!("Sending generated tx to schedulers");
//                 self.new_tx.send(generated_tx)?;
//             }
//         }
//         // println!("Execution is done");
//         anyhow::Ok(duration)
//     }
//
//     pub fn execute_round(&self, mut round: Vec<Transaction>, functions: &Vec<AtomicFunction>, shared_storage: SharedStorage) -> Vec<Transaction> {
//         let chunk_size = self.get_chunk_size(round.len());
//
//         // TODO replace flatmap with map + for loop?
//         round
//             // .into_par_iter()
//             // .par_drain(..round.len())
//             // .chunks(chunk_size)
//             .par_chunks(chunk_size)
//             .enumerate()
//             .flat_map(
//                 |(worker_index, worker_backlog)|
//                     ExecutorSingleton::execute_chunk(worker_backlog, functions, shared_storage)
//             ).collect()
//     }
//
//     fn get_chunk_size(&self, round_size: usize) -> usize {
//         round_size/self.nb_workers + 1
//         // if round_size >= self.nb_workers {
//         //     round_size/self.nb_workers + 1
//         // } else {
//         //     round_size
//         // }
//     }
// }
//
// struct ExecutorSingleton {}
//
// impl ExecutorSingleton {
//     fn execute_chunk(mut worker_backlog: &[Transaction], functions: &Vec<AtomicFunction>, shared_storage: SharedStorage) -> Vec<Transaction> {
//
//         let w = Instant::now();
//         let res: Vec<_> = worker_backlog
//             .into_iter()
//             .flat_map(|tx| ExecutorSingleton::execute_tx(tx, functions, shared_storage))
//             .collect();
//         // println!("\tSingle exec variant took {:?}", w.elapsed());
//         res
//     }
//
//     #[inline]
//     fn execute_tx(tx: &Transaction, functions: &Vec<AtomicFunction>, shared_storage: SharedStorage) -> Option<Transaction> {
//         // execute the transaction and optionally generate a new tx
//         // let function = functions.get(tx.function as usize).unwrap();
//         let function = tx.function;
//         match unsafe { function.execute(tx.clone(), shared_storage) } {
//             Another(generated_tx) => Some(generated_tx),
//             _ => None,
//         }
//     }
// }
// //endregion
//
// //region Scheduling
// #[derive(Debug)]
// struct SchedulingPool {
//     nb_schedulers: usize,
//     // sets: Vec<AddressSet>,
//
//     input: Receiver<Vec<Transaction>>,
//
//     tx_nb_rounds: Sender<usize>,
//     outputs: Vec<Sender<Vec<Transaction>>>,
//     batch_done: Receiver<()>
// }
//
// impl SchedulingPool {
//     fn new(nb_schedulers: usize,
//            // sets: Vec<AddressSet>,
//            input: Receiver<Vec<Transaction>>,
//            tx_nb_rounds: Sender<usize>,
//            outputs: Vec<Sender<Vec<Transaction>>>,
//            batch_done: Receiver<()>) -> Self {
//         Self {
//             nb_schedulers,
//             // sets,
//             input,
//             tx_nb_rounds,
//             outputs,
//             batch_done
//         }
//     }
//
//     fn schedule(&mut self) -> anyhow::Result<Duration> {
//         let mut duration = Duration::from_secs(0);
//
//         let mut sets: Vec<_> = (0..self.nb_schedulers).map(|_| AddressSet::with_capacity(MAX_NB_ADDRESSES * 65536)).collect();
//         'scheduling: loop {
//             // println!("Schedulers waiting for new input");
//             select! {
//                 recv(self.input) -> received => {
//                     if let Ok(mut backlog) = received {
//                         let scheduling_start = Instant::now();
//                         while !backlog.is_empty() {
//                             let chunk_size = self.get_chunk_size(backlog.len());
//                             let mut chunks = backlog.par_drain(..backlog.len()).chunks(chunk_size);
//
//                             // println!("Sending nb of rounds");
//                             if let Err(e) = self.tx_nb_rounds.send(chunks.len()) {
//                                 panic!("Failed to send nb of rounds");
//                             }
//
//                             let tmp: Vec<_> = chunks
//                                 .zip(self.outputs.par_iter_mut())
//                                 .zip(sets.par_iter_mut())
//                                 .enumerate()
//                                 .map(|(scheduler_index, ((chunk, output), set))| {
//
//                                     let (scheduled, postponed) = Scheduler::schedule_chunk(chunk, set);
//                                     // println!("Scheduler {} sending a round", scheduler_index);
//                                     if let Err(e) = output.send(scheduled) {
//                                         panic!("Failed to send schedule");
//                                     }
//                                     postponed
//                                 }).collect();
//
//                             // TODO There is a bug here?
//
//                             // println!("Growing schedulers backlog");
//                             for mut postponed in tmp {
//                                 backlog.append(&mut postponed);
//                             }
//                             // println!("Schedulers waiting for new input");
//                         }
//
//                         duration += scheduling_start.elapsed();
//                     }
//                 },
//                 recv(self.batch_done) -> _ => {
//                     break 'scheduling;
//                 }
//             }
//         }
//         anyhow::Ok(duration)
//
//         // while let Ok(mut backlog) = self.input.recv() {
//         //     let scheduling_start = Instant::now();
//         //
//         //     while !backlog.is_empty() {
//         //         let chunk_size = self.get_chunk_size(backlog.len());
//         //         let mut chunks = backlog.par_drain(..backlog.len()).chunks(chunk_size);
//         //
//         //         println!("Sending nb of rounds");
//         //         if let Err(e) = self.tx_nb_rounds.send(chunks.len()) {
//         //             panic!("Failed to send nb of rounds");
//         //         }
//         //
//         //         let tmp: Vec<_> = chunks
//         //             .zip(self.outputs.par_iter_mut())
//         //             .zip(self.sets.par_iter_mut())
//         //             .enumerate()
//         //             .map(|(scheduler_index, ((chunk, output), set))| {
//         //
//         //                 let (scheduled, postponed) = Scheduler::schedule_chunk(chunk, set);
//         //                 println!("Scheduler {} sending a round", scheduler_index);
//         //                 if let Err(e) = output.send(scheduled) {
//         //                     panic!("Failed to send schedule");
//         //                 }
//         //                 postponed
//         //             }).collect();
//         //
//         //         // TODO There is a bug here?
//         //
//         //         println!("Growing schedulers backlog");
//         //         for mut postponed in tmp {
//         //             backlog.append(&mut postponed);
//         //         }
//         //         println!("Schedulers waiting for new input");
//         //     }
//         //
//         //     duration += scheduling_start.elapsed();
//         // }
//         // anyhow::Ok(duration)
//     }
//
//     fn get_chunk_size(&self, backlog_size: usize) -> usize {
//         // 65536/self.nb_schedulers + 1
//         backlog_size/self.nb_schedulers + 1
//         // if backlog_size >= self.nb_schedulers {
//         //     backlog_size/self.nb_schedulers + 1
//         // } else {
//         //     backlog_size
//         // }
//     }
// }
//
//
// struct Scheduler{}
//
// impl Scheduler {
//     fn schedule_chunk(mut chunk: Vec<Transaction>, set: &mut AddressSet) -> (Vec<Transaction>, Vec<Transaction>) {
//         let mut scheduled = Vec::with_capacity(chunk.len());
//         let mut postponed = Vec::with_capacity(chunk.len());
//
//         'outer: for tx in chunk {
//             for addr in tx.addresses.iter() {
//                 if !set.insert(*addr) {
//                     // Can't add tx to schedule
//                     postponed.push(tx);
//                     continue 'outer;
//                 }
//             }
//             scheduled.push(tx);
//         }
//
//         (scheduled, postponed)
//     }
// }
//endregion
