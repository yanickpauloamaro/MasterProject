#![allow(unused_variables)]

use std::cmp::{max, min};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::mem;
use std::ops::Div;
use std::slice::Iter;
use std::sync::{Arc, Mutex};
use std::sync::mpsc::{channel, Receiver, Sender};
use std::thread::sleep;
use std::time::{Duration, Instant};
use anyhow::anyhow;
use futures::{SinkExt, TryFutureExt};

use strum::{IntoEnumIterator, EnumIter};

use hwloc::Topology;
use itertools::{enumerate, Itertools};
use rand::rngs::StdRng;
use rand::seq::{IteratorRandom, SliceRandom};
use rayon::iter::{IntoParallelIterator, ParallelIterator, IndexedParallelIterator, ParallelDrainRange};
use rayon::slice::ParallelSlice;
use crate::{debug, debugging};
use crate::utils::compatible;
use crate::vm::Jobs;
use crate::vm_a::SerialVM;
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
        sleep(Duration::from_nanos(20));

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
        let mut results = vec![FunctionResult::Error; batch.len()];

        let mut storage = SharedStorage{ ptr: self.storage.as_mut_ptr() };
        let start = Instant::now();
        while !batch.is_empty() {
            let mut tx = batch.pop().unwrap();
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

        return Ok(results);
    }
}

#[derive(Debug)]
pub struct ConcurrentVM {
    pub storage: VmStorage,
    functions: Vec<AtomicFunction>,
    nb_schedulers: usize,
    nb_workers: usize,
}

impl ConcurrentVM {
    pub fn new(storage_size: usize, nb_schedulers: usize, nb_workers: usize) -> anyhow::Result<Self> {
        let storage = VmStorage::new(storage_size);
        let functions = AtomicFunction::iter().collect();

        let vm = Self{ storage, functions, nb_schedulers, nb_workers };
        return Ok(vm);
    }

    // Non-deterministic
    pub fn execute(&mut self, mut batch: Vec<ContractTransaction>) -> anyhow::Result<Vec<FunctionResult>> {

        let prefix_size = 0;
        let nb_chunks_per_scheduler = 1;

        let mut results = vec![FunctionResult::Error; batch.len()];

        let mut backlog: Vec<usize> = (0..batch.len()).collect();
        let mut next_backlog: Vec<usize> = Vec::with_capacity(batch.len());

        // let (prefix, suffix) = backlog.split_at(prefix_size);
        let suffix = backlog.split_off(prefix_size);
        let prefix = backlog;

        let (mut schedule_sender, schedule_receiver) = channel();

        let scheduler_backlog_size = suffix.len()/self.nb_schedulers;

        let scheduling = |schedule_sender: Sender<(usize, Vec<usize>)>, to_schedule: Vec<usize>| {
            to_schedule
                .par_chunks(scheduler_backlog_size)
                .enumerate()
                .map_with(
                    schedule_sender,
                    |sender, (scheduler_index, scheduler_backlog)| {
                        let chunks = if scheduler_backlog.len() >= nb_chunks_per_scheduler {
                            let mut chunk_size = scheduler_backlog.len()/nb_chunks_per_scheduler;
                            scheduler_backlog.chunks(chunk_size)
                        } else {
                            scheduler_backlog.chunks(scheduler_backlog.len())
                        };

                        let mut not_scheduled = Vec::new();
                        chunks.enumerate().for_each(|(chunk_index, chunk)| {
                            // TODO schedule tx and return the indexes that can be executed
                            let mut scheduled = Vec::with_capacity(chunk.len());
                            let mut addresses = tinyset::SetUsize::new();   // TODO add capacity

                            'outer: for tx_index in chunk {
                                let tx: &ContractTransaction = batch.get(*tx_index).unwrap();
                                for addr in tx.params.iter() {
                                    if addresses.contains(*addr as usize) {
                                        // Can't add tx to schedule
                                        not_scheduled.push(*tx_index);
                                        continue 'outer;
                                    }
                                }

                                // Can add tx to schedule
                                scheduled.push(*tx_index);
                                for addr in tx.params.iter() {
                                    addresses.insert(*addr as usize);
                                }
                            }

                            // let schedule = chunk;
                            if let Err(e) = sender.send((scheduler_index, scheduled)) {
                                debug!(
                                "Scheduler {} failed to send its {}-th schedule",
                                scheduler_index, chunk_index
                            );
                            }
                            // TODO Deal with tx that were not executed
                        });

                        not_scheduled
                    }
                ).flatten()
                .collect::<Vec<usize>>()
        };

        let execution = |receiver: Receiver<(usize, Vec<usize>)>, prefix: Vec<usize>| {
            for tx_index in prefix.iter() {
                // TODO execute tx
            }

            while let Ok((scheduler_index, scheduled_txs)) = receiver.recv() {
                let tx_per_worker = if scheduled_txs.len() < self.nb_workers {
                    1
                } else {
                    scheduled_txs.len().div(self.nb_workers) + 1
                };

                // Using rayon -------------------------------------------------------------------------
                let workers: Vec<()> = scheduled_txs.par_chunks(tx_per_worker)
                    .map(|worker_jobs| {
                        // TODO execute tx
                    }
                ).collect();

                // TODO add generated tx to backlog via channel
            }
        };

        let _ = rayon::join(
            || execution(schedule_receiver, prefix),
            move || {

                let mut to_schedule = suffix;
                while !to_schedule.is_empty() {
                    to_schedule = scheduling(
                        schedule_sender.clone(),
                        to_schedule
                    );

                    // TODO add newly generated tx to the backlog too (receive from channel)
                }
            },
        );

        Ok(results)
    }

    pub fn execute_2(&mut self, mut batch: Vec<ContractTransaction>) -> anyhow::Result<Vec<FunctionResult>> {
        /*
            TODO Use VecDequeue?: first half contains tx to execute, the rest contains tx to postpone

         */
        let mut results = Vec::with_capacity(batch.len());

        // let mut backlog = batch;
        // let mut next_backlog = Vec::with_capacity(backlog.len());
        // while !backlog.is_empty() {
        //
        //
        // }
        //
        // let mut backlog: Vec<usize> = (0..batch.len()).collect();
        // let mut next_backlog: Vec<usize> = Vec::with_capacity(batch.len());
        //
        // let (mut schedule_sender, schedule_receiver) = channel();
        //
        // let scheduler_backlog_size = backlog.len()/self.nb_schedulers;
        //
        // let scheduling = |schedule_sender: Sender<(usize, Vec<usize>)>, mut to_schedule: Vec<usize>| {
        //
        //     // while !to_schedule.is_empty() {
        //     //     let tmp = schedule_once(to_schedule);
        //     //     let scheduled = tmp.0;
        //     //     if let Err(e) = schedule_sender.send(scheduled) {
        //     //         debug!("Scheduler {} failed to send its {}-th schedule", scheduler_index, chunk_index);
        //     //     }
        //     //     to_schedule = tmp.1;
        //     // }
        //
        //     to_schedule
        //         .par_chunks(scheduler_backlog_size)
        //         .enumerate()
        //         .map(|(scheduler_index, scheduler_backlog)| {
        //             let mut scheduled = Vec::with_capacity(scheduler_backlog.len());
        //             let mut not_scheduled = vec!();
        //             let mut addresses = tinyset::SetUsize::new();   // TODO add capacity
        //
        //             'outer: for tx_index in scheduler_backlog {
        //                 let tx: &ContractTransaction = batch.get(*tx_index).unwrap();
        //                 for addr in tx.params.iter() {
        //                     if addresses.contains(*addr as usize) {
        //                         // Can't add tx to schedule
        //                         not_scheduled.push(tx);
        //                         continue 'outer;
        //                     }
        //                 }
        //
        //                 // Can add tx to schedule
        //                 scheduled.push(tx);
        //                 for addr in tx.params.iter() {
        //                     addresses.insert(*addr as usize);
        //                 }
        //             }
        //
        //             (scheduled, not_scheduled)
        //         }
        //     )
        //     .collect::<Vec<(Vec<usize>, Vec<usize>)>>()
        // };
        //
        // let execution = |receiver: Receiver<(usize, Vec<usize>)>| {
        //
        //     while let Ok((scheduler_index, scheduled_txs)) = receiver.recv() {
        //         let tx_per_worker = if scheduled_txs.len() < self.nb_workers {
        //             1
        //         } else {
        //             scheduled_txs.len().div(self.nb_workers) + 1
        //         };
        //
        //         // Using rayon -------------------------------------------------------------------------
        //         let workers: Vec<()> = scheduled_txs.par_chunks(tx_per_worker)
        //             .map(|worker_jobs| {
        //                 // TODO execute tx
        //             }
        //             ).collect();
        //
        //         // TODO add generated tx to backlog via channel
        //     }
        // };

        // let _ = rayon::join(
        //     || execution(schedule_receiver),
        //     move || {
        //
        //         let mut to_schedule = backlog;
        //         while !to_schedule.is_empty() {
        //             to_schedule = scheduling(
        //                 schedule_sender.clone(),
        //                 to_schedule
        //             );
        //
        //             // TODO add newly generated tx to the backlog too (receive from channel)
        //         }
        //     },
        // );

        // TODO
        // let mut new_transactions = batch;
        // let mut schedule = vec!();

        // let mut i = 0;
        // while !(new_transactions.is_empty() && schedule.is_empty()) {
        //     let start = Instant::now();
        //     let to_schedule = new_transactions.len();
        //     let to_execute = schedule.len();
        //     // println!("Iteration {}: {} tx to schedule and {} rounds to execute",
        //     //     i, new_transactions.len(), schedule.len());
        //     // // IntelliJ doesn't know that this is ok
        //     // (schedule, new_transactions) = rayon::join(
        //     //     move || scheduling(new_transactions),
        //     //     move || execution(schedule)
        //     // );
        //     let tmp = rayon::join(
        //         move || scheduling(new_transactions),
        //         move || execution(schedule)
        //     );
        //     schedule = tmp.0;
        //     new_transactions = tmp.1;
        //
        //     let elapsed = start.elapsed();
        //     println!("Iteration {}:", i);
        //     println!("\tScheduling {} tx -> {} rounds of execution", to_schedule, schedule.len());
        //     println!("\tExecuting {} rounds of tx -> {} new tx to schedule", to_execute, new_transactions.len());
        //     println!("Took {:?}", elapsed);
        //     println!();
        //     // println!("Iteration {} took {:?}:", i, start.elapsed());
        //     // println!("\tTried to schedule {}")
        //     i += 1;
        // }

        Ok(results)
    }

    pub fn execute_3(&mut self, mut batch: Vec<ContractTransaction>) -> anyhow::Result<Vec<FunctionResult>> {

        let scheduler_chunk_size = batch.len() / self.nb_schedulers;
        let worker_chunk_size = scheduler_chunk_size / self.nb_workers;

        let nb_schedulers = self.nb_schedulers;
        let nb_workers = self.nb_workers;

        let scheduling = move |mut backlog: Vec<ContractTransaction>| {
            let mut schedule: Vec<Vec<ContractTransaction>> = Vec::with_capacity(nb_schedulers);
            while !backlog.is_empty() {

                // TODO Choose between static and dynamic chunk sizes
                let chunk_size = backlog.len();
                // let chunk_size = backlog.len()/nb_schedulers;

                let rounds: Vec<_> = backlog
                    .par_drain(..backlog.len())
                    // .into_par_iter() // Can't reuse the capacity of backlog with into_iter...
                    .chunks(chunk_size)
                    .enumerate()
                    .map(|(scheduler_index, chunk)| {
                        let mut scheduled = Vec::with_capacity(chunk.len());
                        let mut postponed = vec!();

                        let mut working_set = tinyset::SetUsize::new();
                        'outer: for tx in chunk {
                            for addr in tx.addresses.iter() {
                                if working_set.contains(*addr as usize) {
                                    // Can't add tx to schedule
                                    postponed.push(tx);
                                    continue 'outer;
                                }
                            }

                            // Can add tx to schedule
                            for addr in tx.addresses.iter() {
                                working_set.insert(*addr as usize);
                            }
                            scheduled.push(tx);
                        }
                        (scheduled, postponed)
                    }).collect();

                // backlog = vec!();
                for (round, mut postponed) in rounds {
                    // println!("There are still {} tx to schedule", postponed.len());
                    schedule.push(round);
                    backlog.append(&mut postponed);
                }
            }

            schedule
        };

        let execution = |mut schedule: Vec<Vec<ContractTransaction>>, storage: SharedStorage| {
            // TODO Choose between static and dynamic chunk sizes
            let get_chunk_size: fn(usize, usize)->usize = |round_size, nb_workers| {
                if round_size >= nb_workers {
                    round_size/nb_workers + 1
                } else {
                    1
                    // round_size
                }
            };

            let execute_tx = |tx: &ContractTransaction| {
                // execute the transaction and optionally generate a new tx
                let function = self.functions.get(tx.function as usize).unwrap();
                match unsafe { function.execute(tx.clone(), storage) } {
                    Another(generated_tx) => Some(generated_tx),
                    _ => None,
                }
            };

            let run_worker = |(worker_index, mut worker_backlog): (usize, &[ContractTransaction])| {
                // execute transactions sequentially and collect the results
                let worker_generated_tx: Vec<_> = worker_backlog
                    // .drain(..worker_backlog.len())
                    .into_iter()
                    .flat_map(execute_tx)
                    .collect();
                worker_generated_tx
            };

            let execute_round = |(round_index, mut round): (usize, Vec<ContractTransaction>)| {
                let chunk_size = get_chunk_size(round.len(), nb_workers);
                let round_generated_tx: Vec<_> = round
                    // .into_par_iter()
                    // .par_drain(..round.len())
                    // .chunks(chunk_size)
                    .par_chunks(chunk_size)
                    .enumerate()
                    .flat_map(run_worker)
                    .collect();
                round_generated_tx
            };

            // For the given schedule: sequentially execute each round an collect the results
            let new_transactions: Vec<_> = schedule
                // .drain(..schedule.len())
                .into_iter()
                .enumerate()
                .flat_map(execute_round)
                .collect();

            new_transactions
        };

        let mut new_transactions = batch;
        while !new_transactions.is_empty() {
            let start = Instant::now();
            let schedule = scheduling(new_transactions);
            println!("Scheduling took {:?}", start.elapsed());

            let start = Instant::now();
            new_transactions = execution(schedule, self.storage.get_shared());
            println!("Execution took {:?}", start.elapsed());
        }

        Ok(vec!())
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
        }

        Ok(results)
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

