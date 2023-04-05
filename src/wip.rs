#![allow(unused_variables)]

use core::slice;
use std::cmp::{max, min};
use std::collections::{HashMap, VecDeque};
use std::mem;
use std::ops::{Add, Div};
use std::sync::mpsc::{channel, Receiver, Sender};
use std::thread::sleep;
use std::time::{Duration, Instant};
use std::vec::IntoIter;

use anyhow::anyhow;
use futures::SinkExt;
// use futures::SinkExt;
use hwloc::Topology;
// use itertools::Itertools;
use rand::rngs::StdRng;
use rand::seq::{IteratorRandom, SliceRandom};
use rayon::iter::{IndexedParallelIterator, ParallelDrainRange, ParallelIterator};
use rayon::slice::ParallelSlice;
use strum::{EnumIter, IntoEnumIterator};

use crate::{debug, debugging};
use crate::utils::compatible;
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
        // sleep(Duration::from_nanos(20));

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
#[derive(Clone, Debug)]
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
        println!("Sequential took {:?}", start.elapsed());

        return Ok(vec!());
    }
}

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
        2 * 65536
        // 2 * chunk_size
        // 8 * chunk_size
    }

    fn schedule_chunk(&self, scheduler_index: usize, mut chunk: Vec<ContractTransaction>)
                      -> (Vec<ContractTransaction>, Vec<ContractTransaction>) {
        let mut scheduled = Vec::with_capacity(chunk.len());
        let mut postponed = Vec::with_capacity(chunk.len());

        let a = Instant::now();
        let mut working_set = tinyset::SetU64::with_capacity_and_max(
            self.get_working_set_capacity(chunk.len()),
            self.storage.len() as u64
        );

        'outer: for tx in chunk {
            for addr in tx.addresses.iter() {
                if working_set.contains(*addr as u64) {
                    // Can't add tx to schedule
                    postponed.push(tx);
                    continue 'outer;
                }
            }

            // Can add tx to schedule
            for addr in tx.addresses.iter() {
                working_set.insert(*addr as u64);
            }
            scheduled.push(tx);
        }
        println!("Scheduler {} took {:?} to schedule {} tx", scheduler_index, a.elapsed(), scheduled.len() + postponed.len());

        (scheduled, postponed)
    }

    fn schedule_backlog_single_pass(&self, backlog: &mut Vec<ContractTransaction>)
                                    -> Vec<(usize, (Round, Vec<ContractTransaction>))> {
        // TODO Only return scheduled and postponed
        let chunk_size = self.get_scheduler_chunk_size(backlog.len());

        backlog
            .par_drain(..backlog.len())
            .chunks(chunk_size)
            .enumerate()
            .map(|(scheduler_index, chunk)|
                (scheduler_index, self.schedule_chunk(scheduler_index, chunk))
            ).collect()
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

    fn execute_round(&self, round_index: usize, mut round: Round) -> Vec<ContractTransaction> {
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

    // TODO Replaced by the three "schedule_backlog" variants
    fn multi_pass_schedule(&self, mut backlog: &mut Vec<ContractTransaction>) -> Schedule {
        let mut schedule: Vec<Vec<ContractTransaction>> = Vec::with_capacity(self.nb_schedulers);
        while !backlog.is_empty() {

            let rounds = self.schedule_backlog_single_pass(&mut backlog);

            for (scheduler_index, (round, mut postponed)) in rounds {
                debug!("Scheduler {}: scheduled {}, postponed {}", scheduler_index, round.len(), postponed.len());
                schedule.push(round);
                backlog.append(&mut postponed);
            }
        }

        schedule
    }

    // TODO Remove. Replaced by the 5 "execute_variant"
    // Scheduling: Send scheduled rounds one by one
    // Execution: Send result one by one
    pub fn execute_impl_2(&mut self, mut batch: Vec<ContractTransaction>) -> anyhow::Result<Vec<FunctionResult>> {

        let batch_size = batch.len();

        let nb_schedulers = self.nb_schedulers;
        let fixed_scheduler_chunk_size = batch.len()/nb_schedulers;

        let nb_workers = self.nb_workers;

        let (send_generated_tx, receive_generated_tx) = channel();
        let (send_rounds, receive_rounds) = channel();

        send_generated_tx.send(batch)?;

        let scheduling =
            |new_txs: Receiver<Vec<ContractTransaction>>, mut worker_pool: Sender<Vec<_>>| {
                let mut scheduling_duration = Duration::from_secs(0);

                while let Ok(mut backlog) = new_txs.recv() {

                    let scheduling_start = Instant::now();
                    while !backlog.is_empty() {
                        let rounds = self.schedule_backlog_single_pass(&mut backlog);

                        for (scheduler_index, (round, mut postponed)) in rounds {
                            debug!("Scheduler {}: scheduled {}, postponed {}", scheduler_index, round.len(), postponed.len());
                            // Send each round individually
                            worker_pool.send(round)?;
                            backlog.append(&mut postponed);
                        }
                    }

                    // We scheduled the whole backlog
                    scheduling_duration = scheduling_duration.add(scheduling_start.elapsed());
                }

                anyhow::Ok(scheduling_duration)
        };

        let execution =
            |scheduled_rounds: Receiver<Vec<ContractTransaction>>, mut scheduling_pool: Sender<Vec<ContractTransaction>>| {

            let mut completed_tx = 0;
            let mut round_index = 0;
            let mut exec_duration = Duration::from_secs(0);
            while let Ok(round) = scheduled_rounds.recv() {

                let execution_start = Instant::now();
                let round_size = round.len();

                let generated_tx = self.execute_round(round_index, round);
                // If a transaction has not been generated, the original request was completed
                completed_tx += round_size - generated_tx.len();
                debug!("\t{} tx were executed to completion", round_size - generated_tx.len());
                round_index += 1;

                if !generated_tx.is_empty() {
                    scheduling_pool.send(generated_tx)?;
                }

                exec_duration = exec_duration.add(execution_start.elapsed());

                if completed_tx >= batch_size {
                    break;
                }
            }
            anyhow::Ok(exec_duration)
        };

        let (scheduling_res, execution_res) = rayon::join(
            || scheduling(receive_generated_tx, send_rounds),
            || execution(receive_rounds, send_generated_tx),
        );

        let scheduling_duration = scheduling_res.unwrap();
        let execution_duration = execution_res.unwrap();

        println!("Concurrent took {:?}", scheduling_duration + execution_duration);
        println!("\tScheduling (one-by-one): {:?}", scheduling_duration);
        println!("\tExecution (one-by-one):  {:?}", execution_duration);

        return Ok(vec!());
    }

    // TODO Remove. Replaced by the 5 "execute_variant"
    // Scheduling: Send scheduled rounds together
    // Execution: Send result together
    pub fn execute_impl_3(&mut self, mut batch: Vec<ContractTransaction>) -> anyhow::Result<Vec<FunctionResult>> {

        while !batch.is_empty() {
            let start = Instant::now();
            let schedule = self.multi_pass_schedule(&mut batch);
            let scheduling_duration = start.elapsed();

            let start = Instant::now();

            batch = schedule
                // .drain(..schedule.len())
                .into_iter()
                .enumerate()
                .flat_map(
                    |(round_index, round)|
                        self.execute_round(round_index, round)
                ).collect();
            let execution_duration = start.elapsed();

            println!("Manual impl took {:?}", scheduling_duration + execution_duration);
            println!("\tScheduling (grouped) {:?}", start.elapsed());
            println!("\tExecution (grouped) {:?}", start.elapsed());
        }

        Ok(vec!())
    }

    // TODO Remove. Replaced by the 5 "execute_variant"
    // Scheduling: Send scheduled rounds together
    // Execution: Send result one by one
    pub fn execute_impl_4(&mut self, mut batch: Vec<ContractTransaction>) -> anyhow::Result<Vec<FunctionResult>> {

        let batch_size = batch.len();

        let nb_schedulers = self.nb_schedulers;
        let fixed_scheduler_chunk_size = batch.len()/nb_schedulers;

        let nb_workers = self.nb_workers;

        let (send_generated_tx, receive_generated_tx) = channel();
        let (send_rounds, receive_rounds) = channel();

        send_generated_tx.send(batch)?;

        let scheduling =
            |new_txs: Receiver<Vec<ContractTransaction>>, mut worker_pool: Sender<Vec<Vec<ContractTransaction>>>| {
                let mut scheduling_duration = Duration::from_secs(0);

                while let Ok(mut backlog) = new_txs.recv() {
                    let scheduling_start = Instant::now();

                    let schedule = self.multi_pass_schedule(&mut backlog);

                    worker_pool.send(schedule)?;
                    scheduling_duration = scheduling_duration.add(scheduling_start.elapsed());
                }

                anyhow::Ok(scheduling_duration)
            };

        let execution =
            |scheduled_rounds: Receiver<Vec<Vec<ContractTransaction>>>, mut scheduling_pool: Sender<Vec<ContractTransaction>>| {
                let mut completed_tx = 0;
                let mut round_index = 0;
                let mut exec_duration = Duration::from_secs(0);

                while let Ok(schedule) = scheduled_rounds.recv() {

                    let execution_start = Instant::now();
                    for round in schedule {
                        let round_size = round.len();

                        let generated_tx = self.execute_round(round_index, round);
                        // If a transaction has not been generated, the original request was completed
                        completed_tx += round_size - generated_tx.len();

                        debug!("\t{} tx were executed to completion", round_size - generated_tx.len());

                        if !generated_tx.is_empty() {
                            scheduling_pool.send(generated_tx)?;
                        }
                        round_index += 1;
                    }

                    exec_duration = exec_duration.add(execution_start.elapsed());

                    if completed_tx >= batch_size {
                        break;
                    }
                }
                anyhow::Ok(exec_duration)
            };

        let (scheduling_res, execution_res) = rayon::join(
            || scheduling(receive_generated_tx, send_rounds),
            || execution(receive_rounds, send_generated_tx),
        );

        let scheduling_duration = scheduling_res.unwrap();
        let execution_duration = execution_res.unwrap();

        println!("Concurrent took {:?}", scheduling_duration + execution_duration);
        println!("\tScheduling (grouped): {:?}", scheduling_duration);
        println!("\tExecution (one-by-one):  {:?}", execution_duration);

        return Ok(vec!());
    }

    // TODO Remove. Replaced by the 5 "execute_variant"
    // Scheduling: Send scheduled rounds together
    // Execution: Send result together
    pub fn execute_impl_5(&mut self, mut batch: Vec<ContractTransaction>) -> anyhow::Result<Vec<FunctionResult>> {

        let batch_size = batch.len();

        let nb_schedulers = self.nb_schedulers;
        let fixed_scheduler_chunk_size = batch.len()/nb_schedulers;

        let nb_workers = self.nb_workers;

        let (send_generated_tx, receive_generated_tx) = channel();
        let (send_rounds, receive_rounds) = channel();

        send_generated_tx.send(batch)?;

        let scheduling =
            |new_txs: Receiver<Vec<ContractTransaction>>, mut worker_pool: Sender<Vec<Vec<ContractTransaction>>>| {
                let mut scheduling_duration = Duration::from_secs(0);

                while let Ok(mut backlog) = new_txs.recv() {
                    let scheduling_start = Instant::now();

                    let schedule = self.multi_pass_schedule(&mut backlog);

                    worker_pool.send(schedule)?;
                    scheduling_duration = scheduling_duration.add(scheduling_start.elapsed());
                }

                anyhow::Ok(scheduling_duration)
            };

        let execution =
            |scheduled_rounds: Receiver<Vec<Vec<ContractTransaction>>>, mut scheduling_pool: Sender<Vec<ContractTransaction>>| {
                let mut completed_tx = 0;
                let mut round_index = 0;
                let mut exec_duration = Duration::from_secs(0);

                while let Ok(schedule) = scheduled_rounds.recv() {
                    let mut generated_tx = Vec::with_capacity(nb_workers);

                    let execution_start = Instant::now();
                    for round in schedule {
                        let round_size = round.len();

                        let mut round_generated_tx = self.execute_round(round_index, round);
                        // If a transaction has not been generated, the original request was completed
                        completed_tx += round_size - round_generated_tx.len();
                        debug!("\t{} tx were executed to completion", round_size - round_generated_tx.len());

                        generated_tx.append(&mut round_generated_tx);

                        round_index += 1;
                    }

                    //Executed all rounds in the schedule
                    if !generated_tx.is_empty() {
                        scheduling_pool.send(generated_tx)?;
                    }

                    exec_duration = exec_duration.add(execution_start.elapsed());

                    if completed_tx >= batch_size {
                        break;
                    }
                }
                anyhow::Ok(exec_duration)
            };

        let (scheduling_res, execution_res) = rayon::join(
            || scheduling(receive_generated_tx, send_rounds),
            || execution(receive_rounds, send_generated_tx),
        );

        let scheduling_duration = scheduling_res.unwrap();
        let execution_duration = execution_res.unwrap();

        println!("Concurrent took {:?}", scheduling_duration + execution_duration);
        println!("\tScheduling (grouped): {:?}", scheduling_duration);
        println!("\tExecution (grouped):  {:?}", execution_duration);

        return Ok(vec!());
    }
}

//region previous attempt
#[derive(Debug)]
pub struct VM {
    storage: Vec<Word>,
    functions: Vec<AtomicFunction>,
}

impl VM {
    // pub fn new_parallel(storage_size: usize, nb_schedulers: usize, nb_workers: usize) -> anyhow::Result<Self> {
    //     let storage = vec![0; storage_size];
    //     let functions = vec!(AtomicFunction::Transfer);
    //     let vm = Self{ storage, functions, nb_schedulers, nb_workers};
    //     return Ok(vm);
    // }

    // pub fn new_serial(storage_size: usize) -> anyhow::Result<Self> {
    //     let storage = vec![0; storage_size];
    //     let functions = vec!(AtomicFunction::Transfer);
    //     let vm = Self{ storage, functions, 1, 1};
    //     return Ok(vm);
    // }

    pub fn execute(&mut self, mut batch: Batch) -> anyhow::Result<Vec<FunctionResult>> {
        let mut results = vec![FunctionResult::Error; batch.len()];

        for (tx_index, tx) in batch.iter().enumerate() {
            let function = &self.functions[tx.function as usize];
            match function {
                AtomicFunction::Transfer => {
                    let from = tx.params[0] as usize;
                    let to = tx.params[1] as usize;
                    let amount = tx.params[2] as Word;

                    let balance_from = self.storage[from];
                    if balance_from >= amount {
                        self.storage[from] -= amount;
                        self.storage[to] += amount;
                        results[tx_index] = FunctionResult::Success;
                    }
                },
                _ => {

                }
            }
        }

        Ok(results)
    }
}

#[derive(Debug)]
pub struct ParallelContractVM {
    storage: VmStorage,
    functions: Vec<AtomicFunction>,
    nb_workers: usize,
}

impl ParallelContractVM {
    pub fn new(storage_size: usize, nb_workers: usize) -> anyhow::Result<Self> {
        let storage = VmStorage::new(storage_size);
        let functions = vec!(AtomicFunction::Transfer);
        let vm = Self{ storage, functions, nb_workers };
        return Ok(vm);
    }

    pub fn execute(&mut self, mut batch: Batch) -> anyhow::Result<Vec<FunctionResult>> {
        let mut results = vec![FunctionResult::Error; batch.len()];

        loop {
            // assign workers

            // send to workers

            //
            return Ok(results);
        }

    }

    fn crossbeam(
        &mut self,
        results: &mut Vec<FunctionResult>,
        batch: &mut Batch,
        tx_to_worker: &Vec<AssignedWorker>
    ) -> anyhow::Result<()>
    {
        let mut execution_errors: Vec<anyhow::Result<()>> = vec!();

        crossbeam::scope(|s| unsafe {
            let mut shared_storage = self.storage.get_shared();
            let mut shared_results = results.as_mut_ptr();
            let mut handles = Vec::with_capacity(self.nb_workers);

            for i in 0..self.nb_workers {
                let worker_index = i as AssignedWorker + 1;
                // let assignment = batch.iter().zip(tx_to_worker.iter());
                let assigned_tx: Vec<usize> = vec!();
                let batch_ref = &*batch;
                let self_ref = &*self;

                handles.push(s.spawn(move |_| {
                    let mut _accessed = vec!(0);
                    //let _accessed = vec![0; batch.len()];
                    let _worker_name = format!("Worker {}", worker_index);

                    let mut stack: VecDeque<Word> = VecDeque::new();
                    let mut worker_output = vec!();
                    let mut _worker_backlog: Vec<()> = vec!();

                    for tx_index in assigned_tx {
                        let mut tx = batch_ref.get(tx_index).unwrap();
                        let function = &self_ref.functions[tx.function as usize];
                        match function {
                            AtomicFunction::Transfer => {
                                let from = tx.params[0] as usize;
                                let to = tx.params[1] as usize;
                                let amount = tx.params[2] as Word;

                                let balance_from = shared_storage.get(from);
                                if balance_from >= amount {
                                    shared_storage.set(from, balance_from - amount);
                                    let balance_to = shared_storage.get(to);
                                    shared_storage.set(to, balance_to + amount);
                                    // *shared_results.add(tx_index) = ContractResult::Success
                                    worker_output.push((tx_index, FunctionResult::Success))
                                } else {
                                    worker_output.push((tx_index, FunctionResult::Error))
                                }
                            },
                            _ => {

                            }
                        }
                    }

                    (_accessed, worker_output, _worker_backlog)
                }));
            }

            for (_worker_index, handle) in handles.into_iter().enumerate() {
                match handle.join() {
                    Ok((_accessed, mut worker_output, mut worker_backlog)) => {
                        for (tx_index, result) in worker_output {
                            results[tx_index] = result;
                        }

                        // for (tx_index, tx) in worker_backlog {
                        //     batch[tx_index] = tx;
                        // }
                    },
                    Err(e) => {
                        execution_errors.push(Err(anyhow!("{:?}", e)));
                    }
                }
            }
        }).or(Err(anyhow!("Unable to join crossbeam scope")))?;

        return Ok(());
        // if execution_errors.is_empty() {
        //     return Ok(());
        // }
        //
        // return Err(anyhow!("Some error occurred during parallel execution: {:?}", execution_errors));
    }
}

#[derive(Clone, Debug)]
pub struct InternalRequest {
    pub request_index: usize,
    pub contract_index: usize,
    pub function_index: usize,
    pub segment_index: usize,
    pub params: Vec<Word>,
}

#[derive(Clone, Debug)]
pub struct ExternalRequest {
    pub from: Address,
    pub to: Address,
    pub amount: Amount,
    pub data: Data

    // sequence_number,
    // max_gas,
    // gas_unit_price,
}

impl ExternalRequest {
    pub fn transfer(from: Address, to: Address, amount: Amount) -> Self {
        return Self{
            from, to, amount, data: Data::None,
        }
    }

    pub fn call_contract(from: Address, coin_contract: Address, amount: Amount, to: Address) -> Self {
        return Self{
            from,
            to: coin_contract,
            amount: 0,
            data: Data::Parameters(vec!(from, amount, to)),
        }
    }

    pub fn new_coin() -> Self {

        use SegmentInstruction::*;
        let decrement = Segment::new(
            vec!(
                PushParam(1),
                DecrementFromParam(0),  // Return error in case of underflow
                PushParam(2),
                PushParam(1),
                CallSegment(1), // Uses the stack as the params for the next segment
            ),
            vec!(StorageAccess::Param(0))
        );

        let increment = Segment::new(
            vec!(
                PushParam(1),
                IncrementFromParam(0),  // Return error in case of underflow
                Return(0),
            ),
            vec!(StorageAccess::Param(0))
        );

        let transfer= Function::new(vec!(decrement, increment));

        return Self{
            from: 0,
            to: 0,
            amount: 0,
            data: Data::NewContract(vec!(transfer)),
        }
    }

    pub fn batch_with_conflicts(memory_size: usize, batch_size: usize, conflict_rate: f64, mut rng: &mut StdRng) -> Vec<ExternalRequest> {

        let nb_conflict = (conflict_rate * batch_size as f64).ceil() as usize;
        let mut addresses: Vec<u64> = (0..memory_size)
            .choose_multiple(&mut rng, 2*batch_size)
            .into_iter().map(|el| el as u64)
            .collect();

        let mut receiver_occurrences: HashMap<u64, u64> = HashMap::new();
        let mut batch = Vec::with_capacity(batch_size);

        // Create non-conflicting transactions
        for _i in 0..batch_size {
            let amount = 2;

            let from = addresses.pop().unwrap();
            let to = addresses.pop().unwrap();

            // Ensure senders and receivers don't conflict. Otherwise, would need to count conflicts
            // between senders and receivers
            // to += batch_size as u64;

            receiver_occurrences.insert(to, 1);

            let tx = ExternalRequest::transfer(from, to, amount);
            batch.push(tx);
        }

        let indices: Vec<usize> = (0..batch_size).collect();

        let mut conflict_counter = 0;
        while conflict_counter < nb_conflict {
            let i = *indices.choose(&mut rng).unwrap();
            let j = *indices.choose(&mut rng).unwrap();

            if batch[i].to != batch[j].to {

                let freq_i = *receiver_occurrences.get(&batch[i].to).unwrap();
                let freq_j = *receiver_occurrences.get(&batch[j].to).unwrap();

                if freq_j != 2 {
                    if freq_j == 1 { conflict_counter += 1; }
                    if freq_i == 1 { conflict_counter += 1; }

                    receiver_occurrences.insert(batch[i].to, freq_i + 1);
                    receiver_occurrences.insert(batch[j].to, freq_j - 1);

                    batch[j].to = batch[i].to;
                }
            }
        }

        // print_conflict_rate(&batch);

        batch
    }

    pub fn batch_with_conflicts_contract(memory_size: usize, batch_size: usize, conflict_rate: f64, mut rng: &mut StdRng) -> Vec<ExternalRequest> {
        let nb_conflict = (conflict_rate * batch_size as f64).ceil() as usize;
        let mut addresses: Vec<u64> = (0..memory_size)
            .choose_multiple(&mut rng, 2*batch_size)
            .into_iter().map(|el| el as u64)
            .collect();

        let mut receiver_occurrences: HashMap<u64, u64> = HashMap::new();
        let mut batch = Vec::with_capacity(batch_size);

        // Create non-conflicting transactions
        for _i in 0..batch_size {
            let amount = 2;
            let from = addresses.pop().unwrap();
            let to = addresses.pop().unwrap();

            // Ensure senders and receivers don't conflict. Otherwise, would need to count conflicts
            // between senders and receivers
            // to += batch_size as u64;

            receiver_occurrences.insert(to, 1);

            let tx = ExternalRequest::call_contract(from, 0, amount, to);
            batch.push(tx);
        }

        let indices: Vec<usize> = (0..batch_size).collect();

        let receiver = |data: &Data| {
            match data {
                Data::Parameters(v) => v[2],
                _ => panic!("All tx should  parameters in this context")
            }
        };

        let mut conflict_counter = 0;
        while conflict_counter < nb_conflict {
            let i = *indices.choose(&mut rng).unwrap();
            let j = *indices.choose(&mut rng).unwrap();

            if receiver(&batch[i].data) != receiver(&batch[j].data) {

                let freq_i = *receiver_occurrences.get(&receiver(&batch[i].data)).unwrap();
                let freq_j = *receiver_occurrences.get(&receiver(&batch[j].data)).unwrap();

                if freq_j != 2 {
                    if freq_j == 1 { conflict_counter += 1; }
                    if freq_i == 1 { conflict_counter += 1; }

                    receiver_occurrences.insert(batch[i].to, freq_i + 1);
                    receiver_occurrences.insert(batch[j].to, freq_j - 1);

                    batch[j].to = batch[i].to;
                }
            }
        }

        // print_conflict_rate(&batch);

        batch
    }
}

#[derive(Clone, Debug)]
pub enum WipTransactionResult {
    Error,
    TransferSuccess,
    Pending,
    Success(usize),
}

#[derive(Clone, Debug)]
pub enum Data {
    None,
    NewContract(Vec<Function>),
    Parameters(Vec<Param>)
}

pub struct Contract {
    pub storage: VmStorage,
    // main <=> functions[0]
    pub functions: Vec<Function>,
}

#[derive(Clone, Debug)]
pub struct Function {
    pub segments: Vec<Segment>,

    // Function prototype
    // pub prototype: Protoype
}

impl Function {
    pub fn new(segments: Vec<Segment>) -> Self {
        return Self{segments};
    }
}

#[derive(Clone, Debug)]
pub struct Segment {
    pub instructions: Vec<SegmentInstruction>,
    pub accesses: Vec<StorageAccess>
}

impl Segment {

    pub fn new(
        instructions: Vec<SegmentInstruction>,
        accesses: Vec<StorageAccess>
    ) -> Self {
        return Self{instructions, accesses};
    }

    pub fn accessed_addresses(&self, params: &Vec<Param>) -> Vec<usize> {
        return self.accesses.iter().map(|access| {
            match access {
                StorageAccess::Storage(address) => *address,
                StorageAccess::Param(param_index) => params[*param_index] as usize,
            }
        }).collect();
    }

    pub fn accessed_addresses_set(&self, params: &Vec<Param>) -> tinyset::SetU64 {
        let mut set = tinyset::SetU64::with_capacity_and_max(2, 100 * 65536);
        for access in self.accesses.iter() {
            let addr = match access {
                StorageAccess::Storage(address) => *address,
                StorageAccess::Param(param_index) => params[*param_index] as usize,
            };
            set.insert(addr as u64);
        }
        set
    }
    // pub fn accesses(&self, params: &Vec<Param>) -> BTreeMap<Address, AccessType> {
    //
    //     let mut accesses: BTreeMap<Address, AccessType> = BTreeMap::new();
    //     use SegmentInstruction::*;
    //     for instr in self.instructions.iter() {
    //         match instr {
    //             IncrementFromParam(i) => {
    //                 let address = params[*i];
    //                 match accesses.get_mut(&address) {
    //                     Some(mut access) => {
    //                         *access = max(*access, AccessType::Write);
    //                     },
    //                     None => {
    //                         accesses.insert(address, AccessType::Write);
    //                     }
    //                 }
    //             },
    //             DecrementFromParam(i) => {
    //                 let address = params[*i];
    //                 match accesses.get_mut(&address) {
    //                     Some(mut access) => {
    //                         *access = max(*access, AccessType::Write);
    //                     },
    //                     None => {
    //                         accesses.insert(address, AccessType::Write);
    //                     }
    //                 }
    //             }
    //             _ => {
    //
    //             }
    //         }
    //     }
    //
    //     return accesses;
    // }
}
//endregion

#[derive(PartialEq, Eq, PartialOrd, Ord, Clone, Copy, Debug)]
pub enum AccessType {
    Read = 0,
    Write = 1
}

#[derive(Clone, Debug)]
pub enum StorageAccess {
    Storage(usize),
    Param(usize)
    // Accessing using a static address
    // Read(Address),
    // Write(Address),
    // Accessing using a parameter
    // Write(usize),
    // Read(usize),
}

#[derive(Copy, Clone, Debug)]
pub enum SegmentInstruction {
    // TODO Add more instructions (incl. instructions to increase storage size, e.g. vec.resize)
    PushParam(usize),
    IncrementFromParam(usize),
    DecrementFromParam(usize),
    CallSegment(usize),
    Return(u64),
}

// pub struct Interpreter;
//
// impl Interpreter {
//
// }
//
// pub fn test() -> Contract {
//     /*
//     critical {
//         let from = storage[param0]
//         if from < param2 {
//             return 1
//         } else {
//             from = from - param1
//         }
//     }
//
//     go (param1, param2) critical {
//             to = storage[param2]
//             to = to + param1
//             return 0
//         }
//
//
//     critical {
//         let from = storage[param0]  // load from parameter
//         let amount = param1
//         if from < amount {
//             return 1
//         } else {
//             let from = storage[param0]
//             let amount = param1
//             from = from - amount
//             storage[param0] = from
//         }
//     }
//
//     go (param1, param2) critical {
//             to = storage[param2]
//             amount = param1
//             to = to + amount
//             storage[param2] = to
//             return 0
//         }
//      */
//
//     let decrement = Segment{
//         instructions: vec!(
//             LoadFromParam(0),   // push(storage[param0])
//             PushParam(1), // push(param1)
//             Lt,
//             JumpI(+7),
//             LoadFromParam(0),   // push(storage[param0])
//             PushParam(1), // push(param1)
//             Sub,
//             WriteFromParam(0),
//             ParamPopFront,
//             NextSegment,    // is considered a return statement
//             ReturnErr
//         ),
//         accesses: vec!(
//             WriteParam(0)
//         )
//     };
//
//     let increment = Segment{
//         instructions: vec!(
//             LoadFromParam(0),   // push(storage[param0])
//             PushParam(1), // push(param1)
//             Add,
//             WriteFromParam(0),
//             ReturnSuccess
//         ),
//         accesses: vec!(
//             WriteParam(0)
//         )
//     };
//
//     todo!();
// }

// pub fn assign_workers_contracts(
//     vm: &mut SerialVM,
//     nb_workers: usize,
//     batch: &Vec<ExternalRequest>,
//     address_to_worker: &mut Vec<AssignedWorker>,
//     backlog: &mut Vec<ExternalRequest>
// ) -> Vec<AssignedWorker> {
//     let mut tx_to_worker = vec![UNASSIGNED; batch.len()];
//     let mut next_worker = 0 as AssignedWorker;
//     let nb_workers = nb_workers as AssignedWorker;
//     let mut accesses: Vec<usize> = vec!();
//
//     // TODO Need to translate vec of tx into vec of segments...
//     for (index, tx) in batch.iter().enumerate() {
//         let from = tx.from as usize;
//         let to = tx.to as usize;
//
//         if tx.data == Data::None {
//             // Native transfer
//             accesses.push(tx.from as usize);
//             accesses.push(tx.to as usize);
//         } else {
//             let contract = vm.contracts.get(tx.to).unwrap();
//
//         }
//
//         let worker_from = address_to_worker[from];
//         let worker_to = address_to_worker[to];
//
//         if worker_from == UNASSIGNED && worker_to == UNASSIGNED {
//             let assigned = next_worker + 1;
//             address_to_worker[from] = assigned;
//             address_to_worker[to] = assigned;
//             tx_to_worker[index] = assigned;
//             next_worker = if next_worker == nb_workers - 1 {
//                 0
//             } else {
//                 next_worker + 1
//             };
//         } else if worker_from == UNASSIGNED || worker_to == UNASSIGNED {
//             let assigned = max(worker_from, worker_to);
//             address_to_worker[from] = assigned;
//             address_to_worker[to] = assigned;
//             tx_to_worker[index] = assigned;
//
//         } else if worker_from == worker_to {
//             tx_to_worker[index] = worker_from;
//
//         } else {
//             backlog.push(tx.clone());
//         }
//     }
//
//     return tx_to_worker;
// }

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
    print!("Checking compatibility with cpu binding...");
    let topo = Topology::new();
    if let Err(e) = compatible(&topo) {
        println!("\n{} not supported", e);
        return;
    }
    println!("Done.\n");

    println!("Displaying NUMA layout:");
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

