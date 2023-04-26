use std::ops::Add;
use std::sync::Arc;
use std::thread::JoinHandle;
use crossbeam::channel::{Receiver, Sender, unbounded};
// use std::sync::mpsc::{Receiver, Sender, channel};
use crossbeam::select;
use futures::SinkExt;
use itertools::Itertools;
use rayon::prelude::*;
use strum::IntoEnumIterator;
use tokio::time::{Instant, Duration};
use crate::contract::{AtomicFunction, MAX_NB_ADDRESSES, StaticAddress, Transaction};
use crate::contract::FunctionResult::Another;
use crate::vm::Executor;
use crate::vm_utils::{AddressSet, SharedStorage, VmStorage};
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
    pub fn execute<const A: usize, const P: usize>(&mut self, mut batch: Vec<Transaction<A, P>>) -> anyhow::Result<(Duration, Duration)> {
        self.vm.execute_variant_6(batch)
    }

    pub fn set_storage(&mut self, value: Word) {
        self.vm.storage.set_storage(value);
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
    pub fn execute<const A: usize, const P: usize>(&mut self, mut batch: Vec<Transaction<A, P>>) -> anyhow::Result<(Duration, Duration)> {
        self.vm.execute_variant_7(batch)
    }

    pub fn set_storage(&mut self, value: Word) {
        self.vm.storage.set_storage(value);
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
}

impl ParallelVM {
    pub fn new(storage_size: usize, nb_schedulers: usize, nb_workers: usize) -> anyhow::Result<Self> {
        let storage = VmStorage::new(storage_size);
        let functions = AtomicFunction::iter().collect();

        // TODO Try to reuse channels/vectors
        // let (tx_nb_rounds, rx_nb_rounds) = unbounded();
        // let (sets, (scheduling_outputs, execution_inputs)): (Vec<_>, (Vec<_>, Vec<_>)) = (0..nb_schedulers).map(|_| {
        //     (AddressSet::with_capacity(MAX_NB_ADDRESSES * 65536), unbounded())
        // }).unzip();
        //
        // let (new_tx, scheduling_input) = unbounded();
        // let (tx_batch_done, rx_batch_done) = unbounded();

        let scheduler_chunk_size = |backlog_size: usize, nb_schedulers: usize| {
            backlog_size/nb_schedulers + 1
            // 65536/self.nb_schedulers + 1
            // backlog_size/self.nb_schedulers + 1
            // if backlog_size >= self.nb_schedulers {
            //     backlog_size/self.nb_schedulers + 1
            // } else {
            //     backlog_size
            // }
        };

        let executor_chunk_size = |round_size: usize, nb_executors: usize| {
            round_size/nb_executors + 1
            // // (65536/self.nb_schedulers+1)/self.nb_executors + 1
            // round_size/self.nb_executors + 1
            // if round_size >= self.nb_executors {
            //     round_size/self.nb_executors + 1
            // } else {
            //     round_size
            // }
        };

        let vm = Self{ storage, functions, nb_schedulers, scheduler_chunk_size, nb_workers, executor_chunk_size };
        return Ok(vm);
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
        MAX_NB_ADDRESSES * chunk_size
    }

    pub fn schedule_chunk<const A: usize, const P: usize>(&self, mut chunk: Vec<Transaction<A, P>>) -> (Vec<Transaction<A, P>>, Vec<Transaction<A, P>>) {
        let a = Instant::now();
        let mut scheduled = Vec::with_capacity(chunk.len());
        let mut postponed = Vec::with_capacity(chunk.len());

        let mut working_set = AddressSet::with_capacity(
            self.get_address_set_capacity(chunk.len())
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
        // println!("\ttook {:?}", a.elapsed());
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
        round
            .par_chunks(chunk_size)
            .enumerate()
            .flat_map(
                |(worker_index, worker_backlog)|
                    self.execute_chunk(worker_backlog)
            ).collect()

        // TODO Try to let rayon optimise execution
        // round.par_iter().flat_map(|tx| self.execute_tx(tx)).collect()
    }

    pub fn execute_variant_6<const A: usize, const P: usize>(&mut self, mut batch: Vec<Transaction<A, P>>) -> anyhow::Result<(Duration, Duration)> {

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
            let mut duration = Duration::from_secs(0);
            while let Ok(mut backlog) = new_backlog.recv() {
                let scheduling_start = Instant::now();

                while !backlog.is_empty() {
                    let chunk_size = self.get_scheduler_chunk_size(backlog.len());
                    let mut chunks = backlog.par_drain(..backlog.len()).chunks(chunk_size);
                    if chunks.len() > outputs.len() { panic!("Not enough output channels!"); }

                    let res: Vec<_> = chunks
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
                    if res.len() < self.nb_schedulers {
                        if let Err(e) = outputs[res.len()].send(vec!()) {
                            panic!("Failed to send empty schedule: {}", res.len());
                        }
                    }

                    for mut postponed in res {
                        backlog.append(&mut postponed);
                    }
                }

                duration += scheduling_start.elapsed();
            }
            anyhow::Ok(duration)
        };

        let execution = |inputs: Vec<Receiver<Vec<Transaction<A, P>>>>, mut scheduling_pool: Sender<Vec<Transaction<A, P>>>| {
                let mut duration = Duration::from_secs(0);
                let mut executed = 0;
                'outer_loop: loop {
                    let mut generated_tx = vec!();
                    'round_robin: for input in inputs.iter() {
                        if let Ok(round) = input.recv() {
                            let execution_start = Instant::now();
                            if round.is_empty() {
                                break 'round_robin;
                            }

                            executed += round.len();
                            let mut new_tx = self.execute_round(round);
                            executed -= new_tx.len();

                            generated_tx.append(&mut new_tx);
                            duration += execution_start.elapsed();
                        }
                    }

                    if generated_tx.is_empty() {
                        if executed == batch_size {
                            break;
                        }
                    } else if let Err(e) = scheduling_pool.send(generated_tx) {
                        panic!("Failed to send generated tx: {:?}", e);
                    }
                }
                anyhow::Ok(duration)
            };

        let (scheduling_res, execution_res) = rayon::join(
            || scheduling(receive_generated_tx, scheduler_outputs),
            || execution(worker_pool_inputs, send_generated_tx),
        );

        let scheduling_duration = scheduling_res.unwrap();
        let execution_duration = execution_res.unwrap();

        Ok((scheduling_duration, execution_duration))
    }

    pub fn execute_variant_7<const A: usize, const P: usize>(&mut self, mut batch: Vec<Transaction<A, P>>) -> anyhow::Result<(Duration, Duration)> {

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
            let mut duration = Duration::from_secs(0);
            while let Ok(mut backlog) = new_backlog.recv() {
                let scheduling_start = Instant::now();

                while !backlog.is_empty() {
                    let chunk_size = self.get_scheduler_chunk_size(backlog.len());
                    let mut chunks = backlog.par_drain(..backlog.len()).chunks(chunk_size);
                    if chunks.len() > outputs.len() { panic!("Not enough output channels!"); }

                    let res: Vec<_> = chunks
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
                    if res.len() < self.nb_schedulers {
                        if let Err(e) = outputs[res.len()].send(vec!()) {
                            panic!("Failed to send empty schedule: {}", res.len());
                        }
                    }

                    for mut postponed in res {
                        backlog.append(&mut postponed);
                    }
                }

                duration += scheduling_start.elapsed();
            }
            anyhow::Ok(duration)
        };

        // TODO return the channels so that they are not dropped
        let execution = |inputs: Vec<Receiver<Vec<Transaction<A, P>>>>, mut scheduling_pool: Sender<Vec<Transaction<A, P>>>| {
                let mut duration = Duration::from_secs(0);
                let mut executed = 0;
                'outer_loop: loop {
                    'round_robin: for input in inputs.iter() {
                        if let Ok(round) = input.recv() {
                            let execution_start = Instant::now();
                            if round.is_empty() {
                                break 'round_robin;
                            }

                            executed += round.len();
                            let mut new_tx = self.execute_round(round);
                            executed -= new_tx.len();

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
                anyhow::Ok(duration)
            };

        let (scheduling_res, execution_res) = rayon::join(
            || scheduling(receive_generated_tx, scheduler_outputs),
            || execution(worker_pool_inputs, send_generated_tx),
        );

        let scheduling_duration = scheduling_res.unwrap();
        let execution_duration = execution_res.unwrap();

        Ok((scheduling_duration, execution_duration))
    }
}
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
//         let functions = AtomicFunction::iter().collect();
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
