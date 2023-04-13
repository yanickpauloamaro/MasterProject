#![allow(unused_variables)]

use core::slice;
use std::cmp::{max, min};
use std::collections::{HashMap, VecDeque};
use std::mem;
use std::ops::{Add, Div};
use std::sync::{Arc, Mutex};
use std::sync::mpsc::{channel, Receiver, Sender};
use std::thread::sleep;
use std::time::{Duration, Instant};
use std::vec::IntoIter;
use tokio::sync::mpsc::{channel as tokio_channel, Receiver as TokioReceiver, Sender as TokioSender};
use anyhow::anyhow;
use futures::SinkExt;
use itertools::Itertools;
// use futures::SinkExt;
// use hwloc::Topology;
// use itertools::Itertools;
use rand::rngs::StdRng;
use rand::seq::{IteratorRandom, SliceRandom};
use rayon::iter::{IndexedParallelIterator, IntoParallelIterator, ParallelDrainRange, ParallelIterator};
use rayon::slice::ParallelSlice;
use rayon::prelude::*;
use strum::{EnumIter, IntoEnumIterator};
use thincollections::thin_set::ThinSet;
use tokio::sync::RwLock;

use crate::{debug, debugging};
use crate::vm::Jobs;
use crate::vm_utils::{SharedStorage, UNASSIGNED, VmStorage};
use crate::wip::FunctionResult::Another;

pub const CONFLICT_WIP: u8 = u8::MAX;
pub const DONE: u8 = CONFLICT_WIP - 1;
pub const NONE_WIP: u8 = CONFLICT_WIP - 2;

// type ContractValue = u64;
pub type Amount = u64;
pub type AssignedWorker = u8;
pub type Word = u64;
pub type Address = u64;
pub type Param = u64;

pub fn address_translation(addr: &Address) -> usize {
    return *addr as usize;
}


#[derive(Clone, Debug, EnumIter)]
pub enum AtomicFunction {
    Transfer = 0,
    TransferDecrement = 1,
    TransferIncrement = 2,
}

impl AtomicFunction {
    pub unsafe fn execute(
        &self,
        mut tx: ContractTransaction,
        mut storage: SharedStorage
    ) -> FunctionResult {
        let sender = tx.sender;
        let addresses = tx.addresses;
        let params = tx.params;

        // eprintln!("Executing {:?}:", tx);
        // TODO remove the sleep
        // sleep(Duration::from_nanos(2));

        use AtomicFunction::*;
        return match self {
            Transfer => {
                let from = addresses[0] as usize;
                let to = addresses[1] as usize;
                let amount = params[0] as Word;


                // eprintln!("Trying to get balance of {}...", from);
                let balance_from = storage.get(from);
                // eprintln!("Balance of {} is {}", from, balance_from);
                if balance_from >= amount {
                    *storage.get_mut(from) -= amount;
                    *storage.get_mut(to) += amount;
                    FunctionResult::Success
                } else {
                    // eprintln!("Insufficient funds");
                    FunctionResult::ErrorMsg("Insufficient funds")
                }
            },
            TransferDecrement => {
                // Example of function that output another function
                let from = addresses[0] as usize;
                let amount = params[0] as Word;
                if storage.get(from) >= amount {
                    *storage.get_mut(from) -= amount;

                    tx.function = TransferDecrement as FunctionAddress;

                    FunctionResult::Another(tx)
                } else {
                    FunctionResult::ErrorMsg("Insufficient funds")
                }
            },
            TransferIncrement => {
                let to = addresses[1] as usize;
                let amount = params[0] as Word;
                *storage.get_mut(to) += amount;
                FunctionResult::Success
            }
        }
    }
}

#[derive(Clone, Debug)]
pub enum FunctionResult<'a> {
    // Running,
    Success,
    Another(ContractTransaction),
    Error,
    ErrorMsg(&'a str)
}
pub type SenderAddress = u32;
pub type FunctionAddress = u32;
pub type StaticAddress = u32;
pub type FunctionParameter = u32;
pub type Batch = Vec<ContractTransaction>;
const MAX_NB_ADDRESSES: usize = 2;
const MAX_NB_PARAMETERS: usize = 2;

const MAX_TX_SIZE: usize = mem::size_of::<SenderAddress>() +
    mem::size_of::<FunctionAddress>() +
    MAX_NB_PARAMETERS * mem::size_of::<FunctionParameter>();

// TODO Find safe way to have a variable length array?
#[derive(Clone, Debug, Copy)]
pub struct ContractTransaction {
    pub sender: SenderAddress,
    pub function: FunctionAddress,
    pub addresses: [StaticAddress; MAX_NB_ADDRESSES],
    pub params: [FunctionParameter; MAX_NB_PARAMETERS],
}

#[derive(Debug)]
pub struct SequentialVM {
    pub storage: Vec<Word>,
    functions: Vec<AtomicFunction>,
}

impl SequentialVM {
    pub fn new(storage_size: usize) -> anyhow::Result<Self> {
        let storage = vec![0; storage_size];
        let functions = AtomicFunction::iter().collect();

        let vm = Self{ storage, functions };
        return Ok(vm);
    }

    pub fn execute(&mut self, mut batch: Batch) -> anyhow::Result<Vec<FunctionResult>> {

        let storage = SharedStorage{ ptr: self.storage.as_mut_ptr() };
        let start = Instant::now();
        while !batch.is_empty() {
            let tx = batch.pop().unwrap();
            let function = self.functions.get(tx.function as usize).unwrap();
            match unsafe { function.execute(tx, storage) } {
                FunctionResult::Another(generated_tx) => {
                    batch.push(generated_tx);
                },
                _ => {
                    continue;
                }
            }
        }
        // println!("Sequential took {:?}", start.elapsed());

        return Ok(vec!());
    }
}

#[derive(Clone, Debug)]
pub struct ThinSetWrapper {
    inner: ThinSet<StaticAddress>
}
impl ThinSetWrapper {
    pub fn with_capacity_and_max(cap: usize, max: StaticAddress) -> Self {
        let inner = ThinSet::with_capacity(2 * 65536 / 8);
        return Self { inner };
    }
    #[inline]
    pub fn contains(&self, el: StaticAddress) -> bool {
        self.inner.contains(&el)
    }
    #[inline]
    pub fn insert(&mut self, el: StaticAddress) -> bool {
        self.inner.insert(el)
    }
    #[inline]
    pub fn clear(&mut self) {
        self.inner.clear()
    }
}
unsafe impl Send for ThinSetWrapper {}

#[derive(Debug)]
pub struct ConcurrentVM {
    pub storage: VmStorage,
    functions: Vec<AtomicFunction>,
    nb_schedulers: usize,
    nb_workers: usize,
}

pub type Round = Vec<ContractTransaction>;
pub type Schedule = Vec<Round>;

impl ConcurrentVM {
    pub fn new(storage_size: usize, nb_schedulers: usize, nb_workers: usize) -> anyhow::Result<Self> {
        let storage = VmStorage::new(storage_size);
        let functions = AtomicFunction::iter().collect();

        let vm = Self{ storage, functions, nb_schedulers, nb_workers };
        return Ok(vm);
    }

    #[inline]
    fn get_chunk_size(&self, round_size: usize) -> usize {
        if round_size >= self.nb_workers {
            round_size/self.nb_workers + 1
        } else {
            1   // More parallelism but overhead might be bad
            // round_size
        }
    }

    #[inline]
    fn get_scheduler_chunk_size(&self, backlog_size: usize) -> usize {
        if backlog_size >= self.nb_schedulers {
            backlog_size/self.nb_schedulers + 1
        } else {
            1   // More parallelism but overhead might be bad
            // backlog_size
        }
    }

    #[inline]
    fn get_working_set_capacity(&self, chunk_size: usize) -> usize {
        // max(2 * chunk_size, 2 * 65536 / (self.nb_schedulers /2))
        // 2 * 65536
        2 * chunk_size
        // 8 * chunk_size
    }

    fn schedule_chunk(&self, scheduler_index: usize, mut chunk: Vec<ContractTransaction>)
                      -> (Vec<ContractTransaction>, Vec<ContractTransaction>) {
        let mut scheduled = Vec::with_capacity(chunk.len());
        let mut postponed = Vec::with_capacity(chunk.len());

        let a = Instant::now();
        let mut working_set = ThinSetWrapper::with_capacity_and_max(
            self.get_working_set_capacity(chunk.len()),
            self.storage.len() as StaticAddress
        );

        'outer: for tx in chunk {
            for addr in tx.addresses.iter() {
                if !working_set.insert(*addr) {
                    // Can't add tx to schedule
                    postponed.push(tx);
                    continue 'outer;
                }
            }
            scheduled.push(tx);
        }
        // if scheduler_index == 0 {
        //     println!("\tScheduler {} took {:?} to schedule {} tx", scheduler_index, a.elapsed(), scheduled.len() + postponed.len());
        // }
        // println!("\tScheduler {} took {:?} to schedule {} tx", scheduler_index, a.elapsed(), scheduled.len() + postponed.len());

        (scheduled, postponed)
    }

    fn schedule_backlog_single_pass(&self, backlog: &mut Vec<ContractTransaction>)
                                    -> Vec<(usize, (Round, Vec<ContractTransaction>))> {
        // TODO Only return scheduled and postponed
        let chunk_size = self.get_scheduler_chunk_size(backlog.len());
        let a = Instant::now();
        let res: Vec<_> = backlog
            .par_drain(..backlog.len())
            .chunks(chunk_size)
            .enumerate()
            .map(|(scheduler_index, chunk)|
                (scheduler_index, self.schedule_chunk(scheduler_index, chunk))
            ).collect();
        // println!("single_pass took {:?}", a.elapsed());
        res
    }

    // Send rounds one by one
    fn schedule_backlog_variant_1(&self, mut backlog: &mut Vec<ContractTransaction>, worker_pool: &Sender<Round>) -> anyhow::Result<()> {
        while !backlog.is_empty() {

            let rounds = self.schedule_backlog_single_pass(&mut backlog);

            for (scheduler_index, (round, mut postponed)) in rounds {
                debug!("Scheduler {}: scheduled {}, postponed {}", scheduler_index, round.len(), postponed.len());
                worker_pool.send(round)?;
                backlog.append(&mut postponed);
            }
        }

        Ok(())
    }

    // Group rounds and send them after each iteration
    fn schedule_backlog_variant_2(&self, mut backlog: &mut Vec<ContractTransaction>, worker_pool: &Sender<Schedule>) -> anyhow::Result<()> {
        while !backlog.is_empty() {

            let rounds = self.schedule_backlog_single_pass(&mut backlog);

            let mut schedule: Schedule = Vec::with_capacity(rounds.len());

            for (scheduler_index, (round, mut postponed)) in rounds {
                debug!("Scheduler {}: scheduled {}, postponed {}", scheduler_index, round.len(), postponed.len());
                schedule.push(round);
                backlog.append(&mut postponed);
            }

            worker_pool.send(schedule)?;
        }

        Ok(())
    }

    // Group rounds and send them once the backlog is empty
    fn schedule_backlog_variant_3(&self, mut backlog: &mut Vec<ContractTransaction>, worker_pool: &Sender<Schedule>) -> anyhow::Result<()> {
        if backlog.is_empty() {
            return Ok(());
        }

        let mut schedule: Schedule = Vec::with_capacity(self.nb_schedulers);

        while !backlog.is_empty() {

            let rounds = self.schedule_backlog_single_pass(&mut backlog);

            for (scheduler_index, (round, mut postponed)) in rounds {
                debug!("Scheduler {}: scheduled {}, postponed {}", scheduler_index, round.len(), postponed.len());
                schedule.push(round);
                backlog.append(&mut postponed);
            }
        }

        worker_pool.send(schedule)?;

        Ok(())
    }

    fn execute_tx(&self, tx: &ContractTransaction) -> Option<ContractTransaction> {
        // execute the transaction and optionally generate a new tx
        let function = self.functions.get(tx.function as usize).unwrap();
        match unsafe { function.execute(tx.clone(), self.storage.get_shared()) } {
            Another(generated_tx) => Some(generated_tx),
            _ => None,
        }
    }

    fn execute_chunk(&self, worker_index:usize, mut worker_backlog: &[ContractTransaction]) -> Vec<ContractTransaction> {
        worker_backlog
            // .drain(..worker_backlog.len())
            .into_iter()
            .flat_map(|tx| self.execute_tx(tx))
            .collect()
    }

    pub fn execute_round(&self, round_index: usize, mut round: Round) -> Vec<ContractTransaction> {
        let chunk_size = self.get_chunk_size(round.len());
        round
            // .into_par_iter()
            // .par_drain(..round.len())
            // .chunks(chunk_size)
            .par_chunks(chunk_size)
            .enumerate()
            .flat_map(
                |(worker_index, worker_backlog)|
                    self.execute_chunk(worker_index, worker_backlog)
            ).collect()
    }

    fn execute_schedule_variant_1(&self, mut schedule: Schedule, scheduling_pool: &Sender<Vec<ContractTransaction>>) -> anyhow::Result<usize> {
        let mut completed = 0;
        for (round_index, round) in schedule.into_iter().enumerate() {
            completed += round.len();
            let generated_tx = self.execute_round(round_index, round);
            completed -= generated_tx.len();
            scheduling_pool.send(generated_tx)?;
        }
        Ok(completed)
    }

    fn execute_schedule_variant_2(&self, mut schedule: Schedule, scheduling_pool: &Sender<Vec<ContractTransaction>>) -> anyhow::Result<usize> {
        let mut completed = 0;
        let mut generated_tx = vec!();

        for (round_index, round) in schedule.into_iter().enumerate() {
            completed += round.len();
            generated_tx.append(&mut self.execute_round(round_index, round));
        }
        completed -= generated_tx.len();

        scheduling_pool.send(generated_tx)?;

        Ok(completed)
    }

    pub fn execute_variant_1(&mut self, mut batch: Vec<ContractTransaction>) -> anyhow::Result<()> {
        let batch_size = batch.len();

        let (send_generated_tx, receive_generated_tx) = channel();
        let (send_rounds, receive_rounds) = channel();

        send_generated_tx.send(batch)?;

        let scheduling = |new_backlog: Receiver<Vec<ContractTransaction>>, mut worker_pool: Sender<Round>| {
            let mut duration = Duration::from_secs(0);
            while let Ok(mut backlog) = new_backlog.recv() {
                let scheduling_start = Instant::now();
                self.schedule_backlog_variant_1(&mut backlog, &worker_pool)?;
                duration = duration.add(scheduling_start.elapsed());
            }
            anyhow::Ok(duration)
        };

        let execution =
            |next_round: Receiver<Round>, mut scheduling_pool: Sender<Vec<ContractTransaction>>| {
                let mut duration = Duration::from_secs(0);
                let mut executed = 0;
                while let Ok(round) = next_round.recv() {
                    let execution_start = Instant::now();
                    executed += self.execute_schedule_variant_1(vec!(round), &scheduling_pool)?;
                    duration = duration.add(execution_start.elapsed());

                    if executed >= batch_size {
                        break;
                    }
                }
                anyhow::Ok(duration)
            };

        let (scheduling_res, execution_res) = rayon::join(
            || scheduling(receive_generated_tx, send_rounds),
            || execution(receive_rounds, send_generated_tx),
        );

        let scheduling_duration = scheduling_res.unwrap();
        let execution_duration = execution_res.unwrap();

        println!("Variant 1 took {:?}", scheduling_duration + execution_duration);
        println!("\tScheduling (v1): {:?}", scheduling_duration);
        println!("\tExecution (v1):  {:?}", execution_duration);

        Ok(())
    }

    pub fn execute_variant_2(&mut self, mut batch: Vec<ContractTransaction>) -> anyhow::Result<()> {
        let batch_size = batch.len();

        let (send_generated_tx, receive_generated_tx) = channel();
        let (send_rounds, receive_rounds) = channel();

        send_generated_tx.send(batch)?;

        let scheduling = |new_backlog: Receiver<Vec<ContractTransaction>>, mut worker_pool: Sender<Schedule>| {
            let mut duration = Duration::from_secs(0);
            while let Ok(mut backlog) = new_backlog.recv() {
                let scheduling_start = Instant::now();
                self.schedule_backlog_variant_2(&mut backlog, &worker_pool)?;
                duration = duration.add(scheduling_start.elapsed());
            }
            anyhow::Ok(duration)
        };

        let execution =
            |next_round: Receiver<Schedule>, mut scheduling_pool: Sender<Vec<ContractTransaction>>| {
                let mut duration = Duration::from_secs(0);
                let mut executed = 0;
                while let Ok(schedule) = next_round.recv() {
                    let execution_start = Instant::now();
                    executed += self.execute_schedule_variant_1(schedule, &scheduling_pool)?;
                    duration = duration.add(execution_start.elapsed());

                    if executed >= batch_size {
                        break;
                    }
                }
                anyhow::Ok(duration)
            };

        let (scheduling_res, execution_res) = rayon::join(
            || scheduling(receive_generated_tx, send_rounds),
            || execution(receive_rounds, send_generated_tx),
        );

        let scheduling_duration = scheduling_res.unwrap();
        let execution_duration = execution_res.unwrap();

        println!("Variant 2 took {:?}", scheduling_duration + execution_duration);
        println!("\tScheduling (v2): {:?}", scheduling_duration);
        println!("\tExecution (v1):  {:?}", execution_duration);

        Ok(())
    }

    pub fn execute_variant_3(&mut self, mut batch: Vec<ContractTransaction>) -> anyhow::Result<()> {
        let batch_size = batch.len();

        let (send_generated_tx, receive_generated_tx) = channel();
        let (send_rounds, receive_rounds) = channel();

        send_generated_tx.send(batch)?;

        let scheduling = |new_backlog: Receiver<Vec<ContractTransaction>>, mut worker_pool: Sender<Schedule>| {
            let mut duration = Duration::from_secs(0);
            while let Ok(mut backlog) = new_backlog.recv() {
                let scheduling_start = Instant::now();
                self.schedule_backlog_variant_2(&mut backlog, &worker_pool)?;
                duration = duration.add(scheduling_start.elapsed());
            }
            anyhow::Ok(duration)
        };

        let execution =
            |next_round: Receiver<Schedule>, mut scheduling_pool: Sender<Vec<ContractTransaction>>| {
                let mut duration = Duration::from_secs(0);
                let mut executed = 0;
                while let Ok(schedule) = next_round.recv() {
                    let execution_start = Instant::now();
                    executed += self.execute_schedule_variant_2(schedule, &scheduling_pool)?;
                    duration = duration.add(execution_start.elapsed());

                    if executed >= batch_size {
                        break;
                    }
                }
                anyhow::Ok(duration)
            };

        let (scheduling_res, execution_res) = rayon::join(
            || scheduling(receive_generated_tx, send_rounds),
            || execution(receive_rounds, send_generated_tx),
        );

        let scheduling_duration = scheduling_res.unwrap();
        let execution_duration = execution_res.unwrap();

        println!("Variant 3 took {:?}", scheduling_duration + execution_duration);
        println!("\tScheduling (v2): {:?}", scheduling_duration);
        println!("\tExecution (v2):  {:?}", execution_duration);

        Ok(())
    }

    pub fn execute_variant_4(&mut self, mut batch: Vec<ContractTransaction>) -> anyhow::Result<()> {
        let batch_size = batch.len();

        let (send_generated_tx, receive_generated_tx) = channel();
        let (send_rounds, receive_rounds) = channel();

        send_generated_tx.send(batch)?;

        let scheduling = |new_backlog: Receiver<Vec<ContractTransaction>>, mut worker_pool: Sender<Schedule>| {
            let mut duration = Duration::from_secs(0);
            while let Ok(mut backlog) = new_backlog.recv() {
                let scheduling_start = Instant::now();
                self.schedule_backlog_variant_3(&mut backlog, &worker_pool)?;
                duration = duration.add(scheduling_start.elapsed());
            }
            anyhow::Ok(duration)
        };

        let execution =
            |next_round: Receiver<Schedule>, mut scheduling_pool: Sender<Vec<ContractTransaction>>| {
                let mut duration = Duration::from_secs(0);
                let mut executed = 0;
                while let Ok(schedule) = next_round.recv() {
                    let execution_start = Instant::now();
                    executed += self.execute_schedule_variant_1(schedule, &scheduling_pool)?;
                    duration = duration.add(execution_start.elapsed());

                    if executed >= batch_size {
                        break;
                    }
                }
                anyhow::Ok(duration)
            };

        let (scheduling_res, execution_res) = rayon::join(
            || scheduling(receive_generated_tx, send_rounds),
            || execution(receive_rounds, send_generated_tx),
        );

        let scheduling_duration = scheduling_res.unwrap();
        let execution_duration = execution_res.unwrap();

        println!("Variant 4 took {:?}", scheduling_duration + execution_duration);
        println!("\tScheduling (v3): {:?}", scheduling_duration);
        println!("\tExecution (v1):  {:?}", execution_duration);

        Ok(())
    }

    pub fn execute_variant_5(&mut self, mut batch: Vec<ContractTransaction>) -> anyhow::Result<()> {
        let batch_size = batch.len();

        let (send_generated_tx, receive_generated_tx) = channel();
        let (send_rounds, receive_rounds) = channel();

        send_generated_tx.send(batch)?;

        let scheduling = |new_backlog: Receiver<Vec<ContractTransaction>>, mut worker_pool: Sender<Schedule>| {
            let mut duration = Duration::from_secs(0);
            while let Ok(mut backlog) = new_backlog.recv() {
                let scheduling_start = Instant::now();
                self.schedule_backlog_variant_3(&mut backlog, &worker_pool)?;
                duration = duration.add(scheduling_start.elapsed());
            }
            anyhow::Ok(duration)
        };

        let execution =
            |next_round: Receiver<Schedule>, mut scheduling_pool: Sender<Vec<ContractTransaction>>| {
                let mut duration = Duration::from_secs(0);
                let mut executed = 0;
                while let Ok(schedule) = next_round.recv() {
                    let execution_start = Instant::now();
                    executed += self.execute_schedule_variant_2(schedule, &scheduling_pool)?;
                    duration = duration.add(execution_start.elapsed());

                    if executed >= batch_size {
                        break;
                    }
                }
                anyhow::Ok(duration)
            };

        let (scheduling_res, execution_res) = rayon::join(
            || scheduling(receive_generated_tx, send_rounds),
            || execution(receive_rounds, send_generated_tx),
        );

        let scheduling_duration = scheduling_res.unwrap();
        let execution_duration = execution_res.unwrap();

        println!("Variant 5 took {:?}", scheduling_duration + execution_duration);
        println!("\tScheduling (v3): {:?}", scheduling_duration);
        println!("\tExecution (v2):  {:?}", execution_duration);

        Ok(())
    }

    pub async fn execute_variant_6(&mut self, mut batch: Vec<ContractTransaction>) -> anyhow::Result<()> {


        Ok(())
    }
}


#[derive(Debug)]
pub struct BackgroundVM {
    pub storage: VmStorage,
    functions: Arc<RwLock<Vec<AtomicFunction>>>,
    nb_schedulers: usize,
    nb_workers: usize,
}

impl BackgroundVM {
    pub fn new(storage_size: usize, nb_schedulers: usize, nb_workers: usize) -> anyhow::Result<Self> {
        let storage = VmStorage::new(storage_size);
        let functions = Arc::new(RwLock::new(AtomicFunction::iter().collect::<Vec<AtomicFunction>>()));

        let channel_size = 10;
        let chunk_size = 65536/nb_schedulers + 1;
        let max_addr_per_tx = 2;

        let mut scheduler_inputs: Vec<TokioReceiver<VecDeque<ContractTransaction>>> = Vec::with_capacity(nb_schedulers);
        let mut scheduler_outputs: Vec<TokioSender<VecDeque<ContractTransaction>>> = Vec::with_capacity(nb_schedulers);

        let mut worker_pool_inputs = Vec::with_capacity(nb_schedulers);
        let mut worker_pool_outputs = Vec::with_capacity(nb_schedulers);

        // let mut worker_inputs: Vec<TokioReceiver<VecDeque<ContractTransaction>>> = Vec::with_capacity(nb_workers);
        // let mut worker_outputs: Vec<TokioSender<VecDeque<ContractTransaction>>> = Vec::with_capacity(nb_workers);

        for _ in 0..nb_workers {
            let (pool_out, scheduler_in) = tokio_channel(channel_size);
            worker_pool_outputs.push(pool_out);
            scheduler_inputs.push(scheduler_in);

            let (scheduler_out, pool_in) = tokio_channel(channel_size);
            scheduler_outputs.push(scheduler_out);
            worker_pool_inputs.push(pool_in);

            // let (worker_out, worker_in) = tokio_channel(1);
            // worker_inputs.push(worker_in);
            // worker_outputs.push(worker_out);
        }

        let (send_batch, mut receive_batch): (TokioSender<Vec<ContractTransaction>>, TokioReceiver<Vec<ContractTransaction>>) = tokio_channel(1);

        // TODO Graceful shutdown

        let shared_storage = storage.get_shared();
        let shared_functions = functions.clone();

        let worker_pool = tokio::spawn(async move {
            let mut nb_remaining_tx = vec![0 as u32; nb_schedulers];
            let mut backlog: VecDeque<ContractTransaction> = VecDeque::with_capacity(2 * chunk_size);
            let mut vec_pool = Vec::with_capacity(1);

            while let Some(mut batch) = receive_batch.recv().await {
                // TODO Send work to all schedulers
                nb_remaining_tx.fill(0);
                let mut chunks = batch.chunks(chunk_size);
                backlog.extend(chunks.next().unwrap());

                // if chunks.len() > vec_pool.len() {
                //
                // }
                // TODO Collect chunks into vectors from vec_pool (or allocate new vec)
                // for chunk in chunks {
                //
                // }

                let mut next_scheduler = 0;
                'process_batch: loop {
                    for scheduler in 0..nb_schedulers {
                        if nb_remaining_tx[scheduler] > 0 {
                            // We are still waiting on that scheduler
                            match worker_pool_inputs[scheduler].recv().await {
                                Some(mut to_execute) => {
                                    let nb_tx = to_execute.len();
                                    nb_remaining_tx[scheduler] -= nb_tx as u32;

                                    // TODO?
                                    let share_size = nb_tx/nb_workers + 1;
                                    let mut execution_result = to_execute
                                        .par_drain(..nb_tx)
                                        .chunks(share_size)
                                        .flat_map(|worker_backlog| {
                                            let backlog_len = worker_backlog.len();
                                            let mut worker_backlog = VecDeque::from(worker_backlog);
                                            for _ in 0..backlog_len {
                                                let tx = worker_backlog.pop_front().unwrap();
                                                // execute the transaction and optionally generate a new tx
                                                let fs = shared_functions.blocking_read();
                                                let function = (*fs).get(tx.function as usize).unwrap();
                                                match unsafe { function.execute(tx.clone(), shared_storage) } {
                                                    Another(generated_tx) => worker_backlog.push_back(generated_tx),
                                                    _ => (),
                                                }
                                            }
                                            worker_backlog
                                        });

                                    // Should be deterministic, otherwise can collect first to be sure
                                    backlog.par_extend(execution_result);
                                    // backlog.append(&mut execution_result.collect::<VecDeque<ContractTransaction>>());

                                    if backlog.is_empty() {
                                        vec_pool.push(backlog);
                                    } else {
                                        nb_remaining_tx[next_scheduler] += backlog.len() as u32;
                                        if let Err(e) = worker_pool_outputs[next_scheduler].send(backlog).await {
                                            panic!("Scheduler dropped channel!")
                                        }
                                        next_scheduler = if next_scheduler == next_scheduler - 1 {
                                            0
                                        } else {
                                            next_scheduler + 1
                                        }
                                    }

                                    backlog = to_execute;
                                    assert!(backlog.is_empty());
                                },
                                None => panic!("Scheduler dropped channel!")
                            }
                        }
                    }

                    let remaining_tx = nb_remaining_tx.iter().fold(0 as u32, |a, b| a.add(b));
                    if remaining_tx == 0 {
                        break 'process_batch;
                    }
                }
            }
        });

        let scheduler_pool: Vec<_> = scheduler_inputs.into_iter()
            .zip(scheduler_outputs.into_iter())
            .map(|(mut input, output)| {
            tokio::spawn(async move {

                let mut set = ThinSetWrapper::with_capacity_and_max(chunk_size * max_addr_per_tx, storage_size as StaticAddress);
                let mut backlog: VecDeque<ContractTransaction> = VecDeque::with_capacity(2 * chunk_size);
                let mut scheduled: VecDeque<ContractTransaction> = VecDeque::with_capacity(2 * chunk_size);

                while let Some(mut to_schedule) = input.recv().await {
                    let backlog_len = backlog.len();
                    'backlog_loop: for i in 0..backlog_len {
                        let tx = backlog.pop_front().unwrap();

                        for addr in tx.addresses.iter() {
                            if !set.insert(*addr) {
                                /* TODO should make a set with the addresses of this tx and check intersection
                                    otherwise, a tx might conflict "with itself" if the same address
                                    appears multiple times in it parameters
                                 */

                                // Tx can't be executed in this round
                                backlog.push_back(tx);
                                continue 'backlog_loop;
                            }
                        }
                        // Tx can be executed this round
                        scheduled.push_back(tx);
                    }

                    'to_schedule_loop: for tx in to_schedule.drain(..to_schedule.len()) {
                        for addr in tx.addresses.iter() {
                            if !set.insert(*addr) {
                                /* TODO Same as previous loop */
                                // Tx can't be executed in this round
                                backlog.push_back(tx);
                                continue 'to_schedule_loop;
                            }
                        }
                        // Tx can be executed this round
                        scheduled.push_back(tx);
                    }

                    assert!(to_schedule.is_empty());
                    assert!(!scheduled.is_empty());

                    if let Err(e) = output.send(scheduled).await {
                        panic!("Unable to send schedule {}", e);
                    }

                    set.clear();
                    // Reuse the capacity of to_schedule for the next iteration
                    scheduled = to_schedule;
                }

                ()
            })
        }).collect();

        let vm = Self{ storage, functions, nb_schedulers, nb_workers };
        return Ok(vm);
    }
}


pub fn assign_workers_new_impl(
    nb_workers: usize,
    batch: &Jobs,
    address_to_worker: &mut Vec<AssignedWorker>,
    backlog: &mut Jobs,
    worker_to_tx: &mut Vec<Vec<usize>>
) {
    // let mut tx_to_worker = vec![UNASSIGNED; batch.len()];
    let mut next_worker = 0 as AssignedWorker;
    let nb_workers = nb_workers as AssignedWorker;

    for (index, tx) in batch.iter().enumerate() {
        let from = tx.from as usize;
        let to = tx.to as usize;

        let worker_from = address_to_worker[from];
        let worker_to = address_to_worker[to];

        if worker_from == UNASSIGNED && worker_to == UNASSIGNED {
            let assigned = next_worker + 1;
            address_to_worker[from] = assigned;
            address_to_worker[to] = assigned;
            // tx_to_worker[index] = assigned;
            worker_to_tx[assigned as usize - 1].push(index);
            next_worker = if next_worker == nb_workers - 1 {
                0
            } else {
                next_worker + 1
            };
        } else if worker_from == UNASSIGNED || worker_to == UNASSIGNED {
            let assigned = max(worker_from, worker_to);
            address_to_worker[from] = assigned;
            address_to_worker[to] = assigned;
            // tx_to_worker[index] = assigned;
            worker_to_tx[assigned as usize - 1].push(index);

        } else if worker_from == worker_to {
            // tx_to_worker[index] = worker_from;
            worker_to_tx[worker_from as usize - 1].push(index);
        } else {
            backlog.push(tx.clone());
        }
    }

    // return worker_to_tx;
}

pub fn assign_workers_new_impl_2(
    nb_workers: usize,
    batch: &Jobs,
    address_to_worker: &mut Vec<AssignedWorker>,
    backlog: &mut Jobs,
    next: &mut Vec<usize>
) -> Vec<usize> {
    // let mut tx_to_worker = vec![UNASSIGNED; batch.len()];
    let mut next_worker = 0 as AssignedWorker;
    let mut head = vec![usize::MAX; nb_workers];
    let mut tail = vec![usize::MAX; nb_workers];
    let nb_workers = nb_workers as AssignedWorker;

    for (tx_index, tx) in batch.iter().enumerate() {
        let from = tx.from as usize;
        let to = tx.to as usize;

        let worker_from = address_to_worker[from];
        let worker_to = address_to_worker[to];

        if worker_from == UNASSIGNED && worker_to == UNASSIGNED {
            let assigned = next_worker + 1;
            address_to_worker[from] = assigned;
            address_to_worker[to] = assigned;

            // TODO store triplet instead of using three arrays
            let worker = next_worker as usize;
            let current = min(tx_index, tail[worker]);
            next[current] = tx_index;
            // tx_to_worker[index] = assigned;
            tail[worker] = tx_index;
            head[worker] = min(tx_index, head[worker]);

            next_worker = if next_worker == nb_workers - 1 {
                0
            } else {
                next_worker + 1
            };
        } else if worker_from == UNASSIGNED || worker_to == UNASSIGNED {
            let assigned = max(worker_from, worker_to);
            address_to_worker[from] = assigned;
            address_to_worker[to] = assigned;
            // // tx_to_worker[index] = assigned;
            let worker = assigned as usize - 1;
            let current = min(tx_index, tail[worker]);
            next[current] = tx_index;
            // tx_to_worker[index] = assigned;
            tail[worker] = tx_index;
            head[worker] = min(tx_index, head[worker]);

        } else if worker_from == worker_to {
            // tx_to_worker[index] = worker_from;
            // assignment_linked_list[worker_from as usize - 1].push(tx_index);
            let worker = worker_from as usize - 1;
            let current = min(tx_index, tail[worker]);
            next[current] = tx_index;
            // tx_to_worker[index] = assigned;
            tail[worker] = tx_index;
            head[worker] = min(tx_index, head[worker]);
        } else {
            backlog.push(tx.clone());
        }
    }

    return head;
}
// =================================================================================================

pub fn numa_latency() {
    // print!("Checking compatibility with cpu binding...");
    // let topo = Topology::new();
    // if let Err(e) = compatible(&topo) {
    //     println!("\n{} not supported", e);
    //     return;
    // }
    // println!("Done.\n");
    //
    // println!("Displaying NUMA layout:");
    //
    // println!("Enter location of the first thread:");
    // println!("Enter location of the second thread:");
    //
    //
    // println!("Enter the number of iterations (<100):");

    // numa_latency();

    // let length = 10;
    // let mut memory = VmMemory::new(length);
    // println!("Before: {:?}", memory);
    //
    // thread::scope(|s| {
    //     let mut shared1 = memory.get_shared();
    //     let mut shared2 = memory.get_shared();
    //
    //     s.spawn(move |_| {
    //         for i in 0..length.clone() {
    //             if i % 2 == 0 {
    //                 shared1.set(i, (2 * i) as u64);
    //             }
    //         }
    //     });
    //
    //     s.spawn(move |_| {
    //         for i in 0..length.clone() {
    //             if i % 2 == 1 {
    //                 shared2.set(i, i as u64);
    //             }
    //         }
    //     });
    //
    // }).unwrap();
    //
    // println!("After: {:?}", memory);

    // for i in 0..100 {
    //     println!("")
    // }
}

