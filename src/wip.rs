#![allow(unused_variables)]
#![allow(unused_mut)]
use std::cmp::{max, min};
use std::collections::VecDeque;
use std::ops::Add;
use std::sync::Arc;
use crossbeam::channel::{Receiver as CrossbeamReceiver, Sender as CrossbeamSender, unbounded};
use std::time::{Duration, Instant};
use tokio::sync::mpsc::{channel as tokio_channel, Receiver as TokioReceiver, Sender as TokioSender};
// use itertools::Itertools;
use rayon::iter::{IndexedParallelIterator, ParallelDrainRange, ParallelIterator};
use rayon::slice::ParallelSlice;
use rayon::prelude::*;
// use rayon::vec::Drain;
use strum::IntoEnumIterator;
use tokio::task::JoinHandle;

use crate::{debug, debugging};
use crate::contract::{AtomicFunction, StaticAddress, Transaction};
use crate::vm::Jobs;
use crate::vm_utils::{AddressSet, UNASSIGNED, VmStorage};
use crate::contract::FunctionResult::Another;

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

// pub type Batch = Vec<Transaction>;


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
        let functions = AtomicFunction::all();

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
        // 65536/self.nb_schedulers + 1
        backlog_size/self.nb_schedulers + 1
        // if backlog_size >= self.nb_schedulers {
        //     backlog_size/self.nb_schedulers + 1
        // } else {
        //     backlog_size
        // }
    }

    #[inline]
    fn get_working_set_capacity(&self, chunk_size: usize) -> usize {
        // max(2 * chunk_size, 2 * 65536 / (self.nb_schedulers /2))
        // 2 * 65536
        2 * chunk_size
        // 8 * chunk_size
    }

    fn schedule_chunk<const A: usize, const P: usize>(&self, scheduler_index: usize, mut chunk: Vec<Transaction<A, P>>)
                      -> (Vec<Transaction<A, P>>, Vec<Transaction<A, P>>) {
        let mut scheduled = Vec::with_capacity(chunk.len());
        let mut postponed = Vec::with_capacity(chunk.len());

        let a = Instant::now();
        let mut working_set = AddressSet::with_capacity_and_max(
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

    fn schedule_backlog_single_pass<const A: usize, const P: usize>(&self, backlog: &mut Vec<Transaction<A, P>>)
                                    -> Vec<(usize, (Vec<Transaction<A, P>>, Vec<Transaction<A, P>>))> {
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
    fn schedule_backlog_variant_1<const A: usize, const P: usize>(&self, mut backlog: &mut Vec<Transaction<A, P>>, worker_pool: &CrossbeamSender<Vec<Transaction<A, P>>>) -> anyhow::Result<()> {
        while !backlog.is_empty() {

            let rounds = self.schedule_backlog_single_pass(&mut backlog);

            for (scheduler_index, (round, mut postponed)) in rounds {
                // debug!("Scheduler {}: scheduled {}, postponed {}", scheduler_index, round.len(), postponed.len());
                worker_pool.send(round)?;
                backlog.append(&mut postponed);
            }
        }

        Ok(())
    }

    // Group rounds and send them after each iteration
    fn schedule_backlog_variant_2<const A: usize, const P: usize>(&self, mut backlog: &mut Vec<Transaction<A, P>>, worker_pool: &CrossbeamSender<Vec<Vec<Transaction<A, P>>>>) -> anyhow::Result<()> {
        while !backlog.is_empty() {

            let rounds = self.schedule_backlog_single_pass(&mut backlog);

            let mut schedule: Vec<Vec<Transaction<A, P>>> = Vec::with_capacity(rounds.len());

            for (scheduler_index, (round, mut postponed)) in rounds {
                // debug!("Scheduler {}: scheduled {}, postponed {}", scheduler_index, round.len(), postponed.len());
                schedule.push(round);
                backlog.append(&mut postponed);
            }

            worker_pool.send(schedule)?;
        }

        Ok(())
    }

    // Group rounds and send them once the backlog is empty
    fn schedule_backlog_variant_3<const A: usize, const P: usize>(&self, mut backlog: &mut Vec<Transaction<A, P>>, worker_pool: &CrossbeamSender<Vec<Vec<Transaction<A, P>>>>) -> anyhow::Result<()> {
        if backlog.is_empty() {
            return Ok(());
        }

        let mut schedule: Vec<Vec<Transaction<A, P>>> = Vec::with_capacity(self.nb_schedulers);

        while !backlog.is_empty() {

            let rounds = self.schedule_backlog_single_pass(&mut backlog);

            for (scheduler_index, (round, mut postponed)) in rounds {
                // debug!("Scheduler {}: scheduled {}, postponed {}", scheduler_index, round.len(), postponed.len());
                schedule.push(round);
                backlog.append(&mut postponed);
            }
        }

        worker_pool.send(schedule)?;

        Ok(())
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

    fn execute_chunk<const A: usize, const P: usize>(&self, worker_index:usize, mut worker_backlog: &[Transaction<A, P>]) -> Vec<Transaction<A, P>> {

        // print!("{}", worker_backlog.len());
        let w = Instant::now();
        let res: Vec<_> = worker_backlog
            // .drain(..worker_backlog.len())
            .into_iter()
            .flat_map(|tx| self.execute_tx(tx))
            .collect();
        // println!("\tSingle exec variant took {:?}", w.elapsed());
        res
    }

    // TODO replace flatmap with map + for loop?
    pub fn execute_round<const A: usize, const P: usize>(&self, round_index: usize, mut round: Vec<Transaction<A, P>>) -> Vec<Transaction<A, P>> {
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

    fn execute_schedule_variant_1<const A: usize, const P: usize>(&self, mut schedule: Vec<Vec<Transaction<A, P>>>, scheduling_pool: &CrossbeamSender<Vec<Transaction<A, P>>>) -> anyhow::Result<usize> {
        let mut completed = 0;
        for (round_index, round) in schedule.into_iter().enumerate() {
            completed += round.len();
            let generated_tx = self.execute_round(round_index, round);
            completed -= generated_tx.len();
            scheduling_pool.send(generated_tx)?;
        }
        Ok(completed)
    }

    // TODO Variant that actually send tx to execute as fast as possible
    // Send rounds one by one
    async fn schedule_backlog_variant_1_async<const A: usize, const P: usize>(mut backlog: &mut Vec<Transaction<A, P>>, worker_pool: &TokioSender<Vec<Transaction<A, P>>>) -> anyhow::Result<()> {
        while !backlog.is_empty() {

            let chunk_size = backlog.len()/8 + 1; //self.get_scheduler_chunk_size(backlog.len());
            // let backlog = Arc::new(backlog);
            let storage_size = 100 * 65536;

            let mut handles = Vec::with_capacity(8);
            for (scheduler_index, chunk_ref) in backlog.chunks(chunk_size).enumerate() {
                let chunk = Vec::from(chunk_ref);
                let handle = handles.push(tokio::spawn(async move {
                    let mut scheduled = Vec::with_capacity(chunk.len());
                    let mut postponed = Vec::with_capacity(chunk.len());

                    let a = Instant::now();
                    let mut working_set = AddressSet::with_capacity_and_max(
                        // self.get_working_set_capacity(chunk.len()),
                        2 * 65536,
                        storage_size as StaticAddress
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
                    // println!("Scheduler {} is done", scheduler_index);
                    (scheduled, postponed)
                }));
            }

            let mut i = 0;
            backlog.truncate(0);
            for handle in handles.drain(..handles.len()) {
                i += 1;
                if let Ok((scheduled, mut postponed)) = handle.await {
                    if let Err(e) = worker_pool.send(scheduled).await {
                        panic!("Failed to send schedule to worker_pool");
                    }
                    backlog.append(&mut postponed);
                }
            }

            // let (send_schedule, receive_schedule) = channel();
            //
            // tokio::spawn(async move {
            //
            // });
            //
            // let _: () = backlog
            //     .par_drain(..backlog.len())
            //     .chunks(chunk_size)
            //     .enumerate()
            //     .map(|(scheduler_index, chunk)|
            //         (scheduler_index, self.schedule_chunk(scheduler_index, chunk))
            //     ).for_each_with(|| send_schedule.clone(), |sender, (scheduler_index, (round, mut postponed))| {
            //     debug!("Scheduler {}: scheduled {}, postponed {}", scheduler_index, round.len(), postponed.len());
            //     if let Err(e) = sender().send(round) {
            //         panic!("Failed to send schedule to worker_pool");
            //     }
            //     backlog.append(&mut postponed);
            // });

            // let rounds = self.schedule_backlog_single_pass(&mut backlog);

            // for (scheduler_index, (round, mut postponed)) in rounds {
            //     debug!("Scheduler {}: scheduled {}, postponed {}", scheduler_index, round.len(), postponed.len());
            //     worker_pool.send(round)?;
            //     backlog.append(&mut postponed);
            // }
        }

        Ok(())
    }

    async fn execute_schedule_variant_1_async<const A: usize, const P: usize>(&self, mut schedule: Vec<Vec<Transaction<A, P>>>, scheduling_pool: &TokioSender<Vec<Transaction<A, P>>>) -> anyhow::Result<usize> {
        let mut completed = 0;
        for (round_index, round) in schedule.into_iter().enumerate() {
            completed += round.len();
            let generated_tx = self.execute_round(round_index, round);
            completed -= generated_tx.len();

            if !generated_tx.is_empty() {
                scheduling_pool.send(generated_tx).await?;
            }
        }
        Ok(completed)
    }

    fn execute_schedule_variant_2<const A: usize, const P: usize>(&self, mut schedule: Vec<Vec<Transaction<A, P>>>, scheduling_pool: &CrossbeamSender<Vec<Transaction<A, P>>>) -> anyhow::Result<usize> {
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

    pub async fn execute_variant_1_async<const A: usize, const P: usize>(&mut self, mut batch: Vec<Transaction<A, P>>) -> anyhow::Result<()> {
        let batch_size = batch.len();

        let (send_generated_tx, receive_generated_tx) = tokio_channel(10);
        let (send_rounds, mut receive_rounds) = tokio_channel(10);

        send_generated_tx.send(batch).await?;

        let s = tokio::spawn(async move {
            let mut duration = Duration::from_secs(0);
            let sender = send_rounds.clone();
            let mut receiver = receive_generated_tx;
            while let Some(mut backlog) = receiver.recv().await {
                if backlog.is_empty() {
                    break;
                }
                let scheduling_start = Instant::now();
                ConcurrentVM::schedule_backlog_variant_1_async(&mut backlog, &sender).await?;
                duration = duration.add(scheduling_start.elapsed());
            }
            anyhow::Ok(duration)
        });
        let mut duration = Duration::from_secs(0);
        let mut executed = 0;
        while let Some(round) = receive_rounds.recv().await {

            let execution_start = Instant::now();
            executed += self.execute_schedule_variant_1_async(vec!(round), &send_generated_tx).await?;
            duration = duration.add(execution_start.elapsed());

            if executed >= batch_size {
                break;
            }
        }

        let execution_res = anyhow::Ok(duration);
        send_generated_tx.send(vec!()).await.unwrap();
        let scheduling_res = s.await.unwrap();

        let scheduling_duration = scheduling_res.unwrap();
        let execution_duration = execution_res.unwrap();

        // println!("Variant 1 async:");
        println!("\tScheduling (v1): {:?}", scheduling_duration);
        println!("\tExecution (v1):  {:?}", execution_duration);
        println!("\tSum: {:?}", scheduling_duration + execution_duration);

        Ok(())
    }

    pub fn execute_variant_1<const A: usize, const P: usize>(&mut self, mut batch: Vec<Transaction<A, P>>) -> anyhow::Result<()> {
        let batch_size = batch.len();

        let (send_generated_tx, receive_generated_tx) = unbounded();
        let (send_rounds, mut receive_rounds) = unbounded();

        send_generated_tx.send(batch)?;

        let scheduling = |new_backlog: CrossbeamReceiver<Vec<Transaction<A, P>>>, mut worker_pool: CrossbeamSender<Vec<Transaction<A, P>>>| {
            let mut duration = Duration::from_secs(0);
            while let Ok(mut backlog) = new_backlog.recv() {
                let scheduling_start = Instant::now();
                self.schedule_backlog_variant_1(&mut backlog, &worker_pool)?;
                duration = duration.add(scheduling_start.elapsed());
            }
            anyhow::Ok(duration)
        };

        let execution =
            |next_round: CrossbeamReceiver<Vec<Transaction<A, P>>>, mut scheduling_pool: CrossbeamSender<Vec<Transaction<A, P>>>| {
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

        // println!("Variant 1:");
        println!("\tScheduling (v1): {:?}", scheduling_duration);
        println!("\tExecution (v1):  {:?}", execution_duration);
        // println!("\tSum: {:?}", scheduling_duration + execution_duration);

        Ok(())
    }

    pub fn execute_variant_2<const A: usize, const P: usize>(&mut self, mut batch: Vec<Transaction<A, P>>) -> anyhow::Result<()> {
        let batch_size = batch.len();

        let (send_generated_tx, receive_generated_tx) = unbounded();
        let (send_rounds, receive_rounds) = unbounded();

        send_generated_tx.send(batch)?;

        let scheduling = |new_backlog: CrossbeamReceiver<Vec<Transaction<A, P>>>, mut worker_pool: CrossbeamSender<Vec<Vec<Transaction<A, P>>>>| {
            let mut duration = Duration::from_secs(0);
            while let Ok(mut backlog) = new_backlog.recv() {
                let scheduling_start = Instant::now();
                self.schedule_backlog_variant_2(&mut backlog, &worker_pool)?;
                duration = duration.add(scheduling_start.elapsed());
            }
            anyhow::Ok(duration)
        };

        let execution =
            |next_round: CrossbeamReceiver<Vec<Vec<Transaction<A, P>>>>, mut scheduling_pool: CrossbeamSender<Vec<Transaction<A, P>>>| {
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

        // println!("Variant 2:");
        println!("\tScheduling (v2): {:?}", scheduling_duration);
        println!("\tExecution (v1):  {:?}", execution_duration);
        // println!("\tSum: {:?}", scheduling_duration + execution_duration);

        Ok(())
    }

    pub fn execute_variant_3<const A: usize, const P: usize>(&mut self, mut batch: Vec<Transaction<A, P>>) -> anyhow::Result<()> {
        let batch_size = batch.len();

        let (send_generated_tx, receive_generated_tx) = unbounded();
        let (send_rounds, receive_rounds) = unbounded();

        send_generated_tx.send(batch)?;

        let scheduling = |new_backlog: CrossbeamReceiver<Vec<Transaction<A, P>>>, mut worker_pool: CrossbeamSender<Vec<Vec<Transaction<A, P>>>>| {
            let mut duration = Duration::from_secs(0);
            while let Ok(mut backlog) = new_backlog.recv() {
                let scheduling_start = Instant::now();
                self.schedule_backlog_variant_2(&mut backlog, &worker_pool)?;
                duration = duration.add(scheduling_start.elapsed());
            }
            anyhow::Ok(duration)
        };

        let execution =
            |next_round: CrossbeamReceiver<Vec<Vec<Transaction<A, P>>>>, mut scheduling_pool: CrossbeamSender<Vec<Transaction<A, P>>>| {
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

        // println!("Variant 3:");
        println!("\tScheduling (v2): {:?}", scheduling_duration);
        println!("\tExecution (v2):  {:?}", execution_duration);
        // println!("\tSum: {:?}", scheduling_duration + execution_duration);

        Ok(())
    }

    pub fn execute_variant_4<const A: usize, const P: usize>(&mut self, mut batch: Vec<Transaction<A, P>>) -> anyhow::Result<()> {
        let batch_size = batch.len();

        let (send_generated_tx, receive_generated_tx) = unbounded();
        let (send_rounds, receive_rounds) = unbounded();

        send_generated_tx.send(batch)?;

        let scheduling = |new_backlog: CrossbeamReceiver<Vec<Transaction<A, P>>>, mut worker_pool: CrossbeamSender<Vec<Vec<Transaction<A, P>>>>| {
            let mut duration = Duration::from_secs(0);
            while let Ok(mut backlog) = new_backlog.recv() {
                let scheduling_start = Instant::now();
                self.schedule_backlog_variant_3(&mut backlog, &worker_pool)?;
                duration = duration.add(scheduling_start.elapsed());
            }
            anyhow::Ok(duration)
        };

        let execution =
            |next_round: CrossbeamReceiver<Vec<Vec<Transaction<A, P>>>>, mut scheduling_pool: CrossbeamSender<Vec<Transaction<A, P>>>| {
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

        // println!("Variant 4:");
        println!("\tScheduling (v3): {:?}", scheduling_duration);
        println!("\tExecution (v1):  {:?}", execution_duration);
        // println!("\tSum: {:?}", scheduling_duration + execution_duration);

        Ok(())
    }

    pub fn execute_variant_5<const A: usize, const P: usize>(&mut self, mut batch: Vec<Transaction<A, P>>) -> anyhow::Result<()> {
        let batch_size = batch.len();

        let (send_generated_tx, receive_generated_tx) = unbounded();
        let (send_rounds, receive_rounds) = unbounded();

        send_generated_tx.send(batch)?;

        let scheduling = |new_backlog: CrossbeamReceiver<Vec<Transaction<A, P>>>, mut worker_pool: CrossbeamSender<Vec<Vec<Transaction<A, P>>>>| {
            let mut duration = Duration::from_secs(0);
            while let Ok(mut backlog) = new_backlog.recv() {
                let scheduling_start = Instant::now();
                self.schedule_backlog_variant_3(&mut backlog, &worker_pool)?;
                duration = duration.add(scheduling_start.elapsed());
            }
            anyhow::Ok(duration)
        };

        let execution =
            |next_round: CrossbeamReceiver<Vec<Vec<Transaction<A, P>>>>, mut scheduling_pool: CrossbeamSender<Vec<Transaction<A, P>>>| {
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

        // println!("Variant 5:");
        println!("\tScheduling (v3): {:?}", scheduling_duration);
        println!("\tExecution (v2):  {:?}", execution_duration);
        // println!("\tSum: {:?}", scheduling_duration + execution_duration);

        Ok(())
    }

    pub fn execute_variant_6<const A: usize, const P: usize>(&mut self, mut batch: Vec<Transaction<A, P>>) -> anyhow::Result<()> {

        let batch_size = batch.len();

        let mut scheduler_outputs = Vec::with_capacity(self.nb_schedulers);
        let mut worker_pool_inputs = Vec::with_capacity(self.nb_schedulers);
        for _ in 0..self.nb_schedulers {
            let (scheduler_out, worker_pool_in) = unbounded();
            scheduler_outputs.push(scheduler_out);
            worker_pool_inputs.push(worker_pool_in);
        }
        let (send_generated_tx, receive_generated_tx) = unbounded();


        send_generated_tx.send(batch)?;

        let scheduling = |new_backlog: crossbeam::channel::Receiver<Vec<Transaction<A, P>>>, mut outputs: Vec<crossbeam::channel::Sender<Vec<Transaction<A, P>>>>| {
            let mut duration = Duration::from_secs(0);
            while let Ok(mut backlog) = new_backlog.recv() {
                let scheduling_start = Instant::now();

                while !backlog.is_empty() {
                    let chunk_size = self.get_scheduler_chunk_size(backlog.len());
                    let mut chunks = backlog.par_drain(..backlog.len()).chunks(chunk_size);

                    let res: Vec<_> = chunks
                        .zip(outputs.par_iter_mut()).enumerate()
                        .map(|(scheduler_index, (chunk, output))| {
                        let (scheduled, postponed) = self.schedule_chunk(scheduler_index, chunk);
                            if let Err(e) = output.send(scheduled) {
                                panic!("Failed to send schedule");
                            }
                            postponed
                    }).collect();

                    /* TODO move the job of splitting a batch into chunks to the executor pool
                        Otherwise if there are less tx than there are schedulers, the executor pool
                        will end up waiting on a channel that will never receive a msg.
                        This is workaround to ensure the executor pool is never stuck
                        */
                    for i in res.len()..self.nb_schedulers {
                        if let Err(e) = outputs[i].send(vec!()) {
                            panic!("Failed to send empty schedule");
                        }
                    }

                    for mut postponed in res {
                        backlog.append(&mut postponed);
                    }
                }

                duration = duration.add(scheduling_start.elapsed());
            }
            anyhow::Ok(duration)
        };

        let execution =
            |inputs: Vec<crossbeam::channel::Receiver<Vec<Transaction<A, P>>>>, mut scheduling_pool: crossbeam::channel::Sender<Vec<Transaction<A, P>>>| {
                let mut duration = Duration::from_secs(0);
                let mut completed = 0;
                loop {
                    let mut generated_tx = vec!();
                    'inner_loop: for input in inputs.iter() {
                        if let Ok(round) = input.recv() {
                            let execution_start = Instant::now();
                            if round.is_empty() {
                                continue 'inner_loop;
                            }
                            let mut new_tx = self.execute_round(0, round);
                            generated_tx.append(&mut new_tx);
                            duration = duration.add(execution_start.elapsed());
                        }
                    }

                    // TODO this is wrong
                    if generated_tx.is_empty() {
                        break;
                    } else {
                        scheduling_pool.send(generated_tx)?;
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

        // println!("Variant 6:");
        println!("\tScheduling (v0): {:?}", scheduling_duration);
        println!("\tExecution (v2):  {:?}", execution_duration);
        // println!("\tSum: {:?}", scheduling_duration + execution_duration);

        Ok(())
    }

    pub fn execute_variant_7<const A: usize, const P: usize>(&mut self, mut batch: Vec<Transaction<A, P>>) -> anyhow::Result<()> {

        let batch_size = batch.len();

        let mut scheduler_outputs = Vec::with_capacity(self.nb_schedulers);
        let mut worker_pool_inputs = Vec::with_capacity(self.nb_schedulers);
        for _ in 0..self.nb_schedulers {
            let (scheduler_out, worker_pool_in) = unbounded();
            scheduler_outputs.push(scheduler_out);
            worker_pool_inputs.push(worker_pool_in);
        }
        let (send_generated_tx, receive_generated_tx) = unbounded();

        send_generated_tx.send(batch)?;

        let scheduling = |new_backlog: crossbeam::channel::Receiver<Vec<Transaction<A, P>>>, mut outputs: Vec<crossbeam::channel::Sender<Vec<Transaction<A, P>>>>| {
            let mut duration = Duration::from_secs(0);
            while let Ok(mut backlog) = new_backlog.recv() {
                let scheduling_start = Instant::now();

                while !backlog.is_empty() {
                    let chunk_size = self.get_scheduler_chunk_size(backlog.len());

                    let res: Vec<_> = backlog.par_drain(..backlog.len()).chunks(chunk_size)
                        .zip(outputs.par_iter_mut()).enumerate()
                        .map(|(scheduler_index, (chunk, output))| {
                            let (scheduled, postponed) = self.schedule_chunk(scheduler_index, chunk);
                            if let Err(e) = output.send(scheduled) {
                                panic!("Failed to send schedule");
                            }
                            postponed
                        }).collect();

                    /* TODO move the job of splitting a batch into chunks to the executor pool
                        Otherwise if there are less tx than there are schedulers, the executor pool
                        will end up waiting on a channel that will never receive a msg.
                        This is workaround to ensure the executor pool is never stuck
                        */
                    for i in res.len()..self.nb_schedulers {
                        if let Err(e) = outputs[i].send(vec!()) {
                            panic!("Failed to send schedule");
                        }
                    }

                    for mut postponed in res {
                        backlog.append(&mut postponed);
                    }
                }

                duration = duration.add(scheduling_start.elapsed());
            }
            anyhow::Ok(duration)
        };

        let execution =
            |inputs: Vec<crossbeam::channel::Receiver<Vec<Transaction<A, P>>>>, mut scheduling_pool: crossbeam::channel::Sender<Vec<Transaction<A, P>>>| {
                let mut duration = Duration::from_secs(0);
                let mut completed = 0;
                loop {
                    // let mut generated_tx = vec!();
                    let mut generated = 0;
                    'inner_loop: for input in inputs.iter() {
                        if let Ok(round) = input.recv() {
                            let execution_start = Instant::now();
                            if round.is_empty() {
                                continue 'inner_loop;
                            }
                            let mut new_tx = self.execute_round(0, round);
                            if !new_tx.is_empty() {
                                generated += new_tx.len();
                                scheduling_pool.send(new_tx)?;
                            }
                            duration = duration.add(execution_start.elapsed());
                        }
                    }
                    // TODO this is wrong
                    if generated == 0{
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

        // println!("Variant 7:");
        println!("\tScheduling (v0): {:?}", scheduling_duration);
        println!("\tExecution (v1):  {:?}", execution_duration);
        // println!("\tSum: {:?}", scheduling_duration + execution_duration);

        Ok(())
    }
}


#[derive(Debug)]
pub struct BackgroundVMDeque<const A: usize, const P: usize> {
    // Can use Vec instead of VecDequeue, only scheduler.backlog_owned needs to be a vecdeque
    pub storage: VmStorage,
    // functions: Arc<std::sync::RwLock<Vec<AtomicFunction>>>,
    functions: Arc<Vec<AtomicFunction>>,

    send_batch: TokioSender<Vec<Transaction<A, P>>>,
    receive_result: TokioReceiver<()>,

    nb_workers: usize,
    worker_pool: Vec<JoinHandle<()>>,

    nb_schedulers: usize,
    scheduler_pool: Vec<JoinHandle<()>>,
}

impl<const A: usize, const P: usize> BackgroundVMDeque<A, P> {
    pub fn new(storage_size: usize, nb_schedulers: usize, nb_workers: usize) -> anyhow::Result<Self> {
        let storage = VmStorage::new(storage_size);
        // let functions = Arc::new(std::sync::RwLock::new(AtomicFunction::iter().collect::<Vec<AtomicFunction>>()));
        let functions = Arc::new(AtomicFunction::all());

        let channel_size = 10;
        let chunk_size = 65536/nb_schedulers + 1;
        let max_addr_per_tx = 2;

        let mut scheduler_inputs: Vec<TokioReceiver<VecDeque<Transaction<A, P>>>> = Vec::with_capacity(nb_schedulers);
        let mut scheduler_outputs: Vec<TokioSender<VecDeque<Transaction<A, P>>>> = Vec::with_capacity(nb_schedulers);

        let mut worker_pool_inputs = Vec::with_capacity(nb_schedulers);
        let mut worker_pool_outputs = Vec::with_capacity(nb_schedulers);

        // let mut worker_inputs: Vec<TokioReceiver<VecDeque<ContractTransaction>>> = Vec::with_capacity(nb_workers);
        // let mut worker_outputs: Vec<TokioSender<VecDeque<ContractTransaction>>> = Vec::with_capacity(nb_workers);

        for _ in 0..nb_schedulers {
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

        let (send_batch, mut receive_batch): (TokioSender<Vec<Transaction<A, P>>>, TokioReceiver<Vec<Transaction<A, P>>>) = tokio_channel(1);
        let (send_result, mut receive_result): (TokioSender<()>, TokioReceiver<()>) = tokio_channel(1);

        // TODO Graceful shutdown

        let shared_storage = storage.get_shared();
        let shared_functions = functions.clone();

        let coordinator = tokio::spawn(async move {
            let mut nb_remaining_tx = vec![0; nb_schedulers];
            let mut backlog: VecDeque<Transaction<A, P>> = VecDeque::with_capacity(2 * chunk_size);
            let mut vec_pool: Vec<VecDeque<Transaction<A, P>>> = Vec::with_capacity(1);
let mut duration = Duration::from_secs(0);
            while let Some(mut batch) = receive_batch.recv().await {
                if batch.is_empty() {
                    break;
                }

                // println!("Coordinator sending work to schedulers");
                nb_remaining_tx.fill(0);
                let mut chunks = batch.chunks(chunk_size);
                // backlog.extend(chunks.next().unwrap());  // !!!

                let mut next_scheduler = 0;
                for chunk in chunks {
                    // let mut to_schedule = vec_pool.pop()
                    //     .unwrap_or_else(|| Vec::with_capacity(2 * chunk_size));
                    // to_schedule.extend_from_slice(chunk);

                    let to_schedule = if vec_pool.is_empty() {
                        let mut tmp = VecDeque::with_capacity(2 * chunk_size);
                        tmp.extend(chunk.iter());
                        tmp
                        // VecDeque::from(Vec::from(chunk))
                    } else {
                        let mut tmp: VecDeque<Transaction<A, P>> = vec_pool.pop().unwrap();
                        tmp.extend(chunk.iter());
                        tmp
                    };

                    nb_remaining_tx[next_scheduler] += to_schedule.len();
                    if let Err(e) = worker_pool_outputs[next_scheduler].send(to_schedule).await {
                        panic!("Failed to send transactions to scheduler");
                    }

                    next_scheduler = if next_scheduler == nb_schedulers - 1 {
                        0
                    } else {
                        next_scheduler + 1
                    }
                }

                // for s in 0..nb_schedulers { // debug
                //     eprintln!("Scheduler {} has {} tx to schedule", s, nb_remaining_tx[s]);
                // }

                let mut next_scheduler = 0;
                let mut round = 0;
                'process_batch: loop {
                    // eprintln!("\nVec<Transaction<A, P>> {} -----------------------------", round);
                    for scheduler in 0..nb_schedulers {
                        // eprint!("Scheduler {}...", scheduler);
                        if nb_remaining_tx[scheduler] > 0 {
                            // We are still waiting on that scheduler
                            match worker_pool_inputs[scheduler].recv().await {
                                Some(mut to_execute) => {
                                    let nb_tx = to_execute.len();

                                    if to_execute.is_empty() { panic!("Should not have pulled channel since nb_remaining_tx should have been 0"); } // !!!
                                    // println!(" sent {} tx to execute", nb_tx);
                                    nb_remaining_tx[scheduler] -= nb_tx;

                                    // TODO?
                                    let a = Instant::now();
                                    let share_size = nb_tx/nb_workers + 1;
                                    // let mut tmp = Vec::from(to_execute);
                                    let mut execution_result = to_execute
                                        .par_drain(..nb_tx)
                                        .chunks(share_size)
                                        // .par_iter()
                                        // .chunks(share_size)
                                        // .par_chunks(share_size)
                                        .flat_map(|worker_backlog| {
                                            print!("{}", worker_backlog.len());
                                            let w = Instant::now();
                                            // let backlog_len = worker_backlog.len();
                                            // let mut worker_backlog = VecDeque::from(worker_backlog);
                                            // for _ in 0..backlog_len {
                                            //     let tx = worker_backlog.pop_front().unwrap();
                                            //     // execute the transaction and optionally generate a new tx
                                            //     // let fs = shared_functions.read().unwrap();
                                            //     // let function = fs.get(tx.function as usize).unwrap();
                                            //     let function = shared_functions.get(tx.function as usize).unwrap();
                                            //     match unsafe { function.execute(tx.clone(), shared_storage) } {
                                            //         Another(generated_tx) => worker_backlog.push_back(generated_tx),
                                            //         _ => (),
                                            //     }
                                            // }
                                            // let res = worker_backlog;

                                            let res: Vec<Transaction<A, P>> = worker_backlog
                                                // .drain(..worker_backlog.len())
                                                .into_iter()
                                                .flat_map(|tx| {
                                                    // let function = shared_functions.get(tx.function as usize).unwrap();
                                                    let function = tx.function;
                                                    match unsafe { function.execute(tx.clone(), shared_storage) } {
                                                        Another(generated_tx) => Some(generated_tx),
                                                        _ => None,
                                                    }
                                                })
                                                .collect();

                                            // sleep(Duration::from_micros(200));
                                            // let res: VecDeque<ContractTransaction> = VecDeque::with_capacity(2 * chunk_size);
                                            println!("\tSingle exec took {:?}", w.elapsed());
                                            res
                                        });

                                    // Should be deterministic, otherwise can collect first to be sure

                                    backlog.par_extend(execution_result);
                                    // duration = duration.add(a.elapsed());
                                    // tmp.truncate(0);
                                    // to_execute = VecDeque::from(tmp);
                                    // backlog.append(&mut execution_result.collect::<VecDeque<ContractTransaction>>());
                                    // duration = duration.add(a.elapsed());
                                    // if backlog.is_empty() {
                                    //     println!("\tNo tx were generated");
                                    //     vec_pool.push(backlog);
                                    //     // println!("1");
                                    // } else {
                                    //     let mut next_scheduler = scheduler;  // TODO Try giving back to scheduler you just executed
                                    //     println!("\tSending {} generated tx to scheduler {}", backlog.len(), next_scheduler);
                                    //     nb_remaining_tx[next_scheduler] += backlog.len();
                                    //     if let Err(e) = worker_pool_outputs[next_scheduler].send(backlog).await {
                                    //         panic!("Failed to send transactions to scheduler");
                                    //     }
                                    //     next_scheduler = if next_scheduler == nb_schedulers - 1 {
                                    //         0
                                    //     } else {
                                    //         next_scheduler + 1
                                    //     }
                                    // }

                                    // TODO Try giving back to scheduler you just executed !!!
                                    // => TODO Need to collect empty vecs and add them to the pool
                                    // println!("\tSending {} generated tx to scheduler {}", backlog.len(), scheduler);
                                    nb_remaining_tx[scheduler] += backlog.len();
                                    if let Err(e) = worker_pool_outputs[scheduler].send(backlog).await {
                                        panic!("Failed to send transactions to scheduler");
                                    }

                                    backlog = to_execute;

                                    assert!(backlog.is_empty());

                                },
                                None => panic!("Scheduler dropped channel!")
                            }
                        } else {
                            // eprintln!(" has no tx to execute");
                        }
                    }
                    let remaining_tx = nb_remaining_tx.iter().fold(0, |a, b| a.add(b));
                    if remaining_tx + backlog.len() == 0 {
                        break 'process_batch;
                    }
                    round += 1;
                }

                // eprintln!();
                // eprintln!("Finished executing batch");
                if let Err(e) = send_result.send(()).await {
                    panic!("Failed to send result");
                }
            }

            eprintln!("Worker pool exiting");
            // println!("Execution took {:?}", duration);
        });

        let worker_pool = vec![coordinator];

        let scheduler_pool: Vec<_> = scheduler_inputs.into_iter()
            .zip(scheduler_outputs.into_iter())
            .enumerate()
            .map(|(scheduler_index, (mut input, output))| {
            tokio::spawn(async move {

                let mut set_owned = AddressSet::with_capacity_and_max(chunk_size * max_addr_per_tx, storage_size as StaticAddress);
                let mut backlog_owned: VecDeque<Transaction<A, P>> = VecDeque::with_capacity(2 * chunk_size);
                let mut scheduled: VecDeque<Transaction<A, P>> = VecDeque::with_capacity(2 * chunk_size);

                // TODO Scheduler is waiting for a new input to merge with its current backlog but the coordinator is waiting on this scheduler to get sth to execute
                // => need to send them empty backlogs...
                while let Some(mut to_schedule) = input.recv().await {
                    if to_schedule.is_empty() && backlog_owned.is_empty() {
                        // TODO Don't do anything !!!
                        // eprintln!("\t\t\t\t\tReturning vec to coordinator vec_pool");
                        if let Err(e) = output.send(to_schedule).await {
                            panic!("Unable to send vec {}", e);
                        }
                        continue;
                    }
                    // eprintln!("\t\t\t\tScheduler {} received {} tx to schedule", scheduler_index, to_schedule.len());
                    let backlog_len = backlog_owned.len();
                    // eprintln!("\t\t\t\t\tScheduling backlog of {} tx first", backlog_len);
                    'backlog_loop: for i in 0..backlog_len {
                        let tx = backlog_owned.pop_front().unwrap();

                        for addr in tx.addresses.iter() {
                            if !set_owned.insert(*addr) {
                                /* TODO should make a set with the addresses of this tx and check intersection
                                    otherwise, a tx might conflict "with itself" if the same address
                                    appears multiple times in it parameters
                                 */

                                // Tx can't be executed in this round
                                backlog_owned.push_back(tx);
                                continue 'backlog_loop;
                            }
                        }
                        // Tx can be executed this round
                        scheduled.push_back(tx);
                    }

                    // eprintln!("\t\t\t\tScheduling new transactions");
                    'to_schedule_loop: for tx in to_schedule.drain(..to_schedule.len()) {
                        for addr in tx.addresses.iter() {
                            if !set_owned.insert(*addr) {
                                /* TODO Same as previous loop */
                                // Tx can't be executed in this round
                                backlog_owned.push_back(tx);
                                continue 'to_schedule_loop;
                            }
                        }
                        // Tx can be executed this round
                        scheduled.push_back(tx);
                    }

                    assert!(to_schedule.is_empty());
                    assert!(!scheduled.is_empty());

                    // eprintln!("\t\t\t\t\tSending schedule of {} tx to coordinator", scheduled.len());
                    if let Err(e) = output.send(scheduled).await {
                        panic!("Unable to send schedule {}", e);
                    }

                    set_owned.clear();
                    // Reuse the capacity of to_schedule for the next iteration
                    scheduled = to_schedule;
                    // eprintln!("\t\t\t\tScheduler {} waiting on coordinator...", scheduler_index);
                }

                eprintln!("Scheduler {} exiting", scheduler_index);

                ()
            })
        }).collect();

        let vm = Self{ storage, functions, send_batch, receive_result, nb_workers, worker_pool, nb_schedulers, scheduler_pool };
        return Ok(vm);
    }

    pub async fn execute(&mut self, mut batch: Vec<Transaction<A, P>>) -> anyhow::Result<()> {

        if let Err(e) = self.send_batch.send(batch).await {
            panic!("Failed to send batch to be executed");
        }

        if let None = self.receive_result.recv().await {
            panic!("Failed to receive result");
        }

        Ok(())
    }

    pub async fn stop(&mut self) {
        println!("Sending stop signal");
        if let Err(e) = self.send_batch.send(vec!()).await {
            panic!("Failed to send stop signal");
        }

        println!("Waiting on schedulers...");
        for scheduler in self.scheduler_pool.drain(..self.nb_schedulers) {
            if let Err(e) = scheduler.await {
                panic!("Failed to join scheduler");
            }
        }

        println!("Waiting on worker pool...");
        for pool in self.worker_pool.drain(..1) {
            if let Err(e) = pool.await {
                panic!("Failed to join worker pool");
            }
        }

        println!("Successfully stopped vm");
    }
}

#[derive(Debug)]
pub struct BackgroundVM<const A: usize, const P: usize> {
    pub storage: VmStorage,
    // functions: Arc<std::sync::RwLock<Vec<AtomicFunction>>>,
    functions: Arc<Vec<AtomicFunction>>,

    send_batch: TokioSender<Vec<Transaction<A, P>>>,
    receive_result: TokioReceiver<()>,

    nb_workers: usize,
    worker_pool: Vec<JoinHandle<()>>,

    nb_schedulers: usize,
    scheduler_pool: Vec<JoinHandle<()>>,
}

impl<const A: usize, const P: usize> BackgroundVM<A, P> {
    pub fn new(storage_size: usize, nb_schedulers: usize, nb_workers: usize) -> anyhow::Result<Self> {
        let storage = VmStorage::new(storage_size);
        // let functions = Arc::new(std::sync::RwLock::new(AtomicFunction::iter().collect::<Vec<AtomicFunction>>()));
        let functions = Arc::new(AtomicFunction::all());

        let channel_size = 10;
        let chunk_size = 65536/nb_schedulers + 1;
        let max_addr_per_tx = 2;

        let mut scheduler_inputs: Vec<TokioReceiver<Vec<Transaction<A, P>>>> = Vec::with_capacity(nb_schedulers);
        let mut scheduler_outputs: Vec<TokioSender<Vec<Transaction<A, P>>>> = Vec::with_capacity(nb_schedulers);

        let mut worker_pool_inputs = Vec::with_capacity(nb_schedulers);
        let mut worker_pool_outputs = Vec::with_capacity(nb_schedulers);

        for _ in 0..nb_schedulers {
            let (pool_out, scheduler_in) = tokio_channel(channel_size);
            worker_pool_outputs.push(pool_out);
            scheduler_inputs.push(scheduler_in);

            let (scheduler_out, pool_in) = tokio_channel(channel_size);
            scheduler_outputs.push(scheduler_out);
            worker_pool_inputs.push(pool_in);
        }

        let (send_batch, mut receive_batch): (TokioSender<Vec<Transaction<A, P>>>, TokioReceiver<Vec<Transaction<A, P>>>) = tokio_channel(1);
        let (send_result, mut receive_result): (TokioSender<()>, TokioReceiver<()>) = tokio_channel(1);

        // TODO Graceful shutdown

        let shared_storage = storage.get_shared();
        let shared_functions = functions.clone();

        let coordinator = tokio::spawn(async move {
            let mut nb_remaining_tx = vec![0; nb_schedulers];
            let mut backlog: Vec<Transaction<A, P>> = Vec::with_capacity(2 * chunk_size);
            let mut vec_pool: Vec<Vec<Transaction<A, P>>> = Vec::with_capacity(1);
            let mut duration = Duration::from_secs(0);
            while let Some(mut batch) = receive_batch.recv().await {
                if batch.is_empty() {
                    break;
                }

                // println!("Coordinator sending work to schedulers");
                nb_remaining_tx.fill(0);
                let mut chunks = batch.chunks(chunk_size);
                // backlog.extend(chunks.next().unwrap());  // !!!

                let mut next_scheduler = 0;
                for chunk in chunks {
                    let mut to_schedule = if vec_pool.is_empty() {
                        Vec::with_capacity(2 * chunk_size)
                    } else {
                        vec_pool.pop().unwrap()
                    };
                    to_schedule.extend(chunk.iter());

                    nb_remaining_tx[next_scheduler] += to_schedule.len();
                    if let Err(e) = worker_pool_outputs[next_scheduler].send(to_schedule).await {
                        panic!("Failed to send transactions to scheduler");
                    }

                    next_scheduler = if next_scheduler == nb_schedulers - 1 {
                        0
                    } else {
                        next_scheduler + 1
                    }
                }

                // for s in 0..nb_schedulers { // debug
                //     eprintln!("Scheduler {} has {} tx to schedule", s, nb_remaining_tx[s]);
                // }

                let mut next_scheduler = 0;
                let mut round = 0;
                'process_batch: loop {
                    // eprintln!("\nVec<Transaction<A, P>> {} -----------------------------", round);
                    for scheduler in 0..nb_schedulers {
                        // eprint!("Scheduler {}...", scheduler);
                        if nb_remaining_tx[scheduler] > 0 {
                            // We are still waiting on that scheduler
                            match worker_pool_inputs[scheduler].recv().await {
                                Some(mut to_execute) => {
                                    let nb_tx = to_execute.len();

                                    if to_execute.is_empty() { panic!("Should not have pulled channel since nb_remaining_tx should have been 0"); } // !!!
                                    // println!(" sent {} tx to execute", nb_tx);
                                    nb_remaining_tx[scheduler] -= nb_tx;

                                    // TODO?
                                    let a = Instant::now();
                                    let share_size = nb_tx/nb_workers + 1;

                                    let w = Instant::now();
                                    let l = to_execute.len();
                                    to_execute.drain(..to_execute.len())
                                        // .drain(..worker_backlog.len())
                                        .for_each(|tx| {
                                            // let function = shared_functions.get(tx.function as usize).unwrap();
                                            let function = AtomicFunction::Transfer;
                                            match unsafe { function.execute(tx.clone(), shared_storage) } {
                                                Another(generated_tx) => backlog.push(generated_tx),
                                                _ => (),
                                            }
                                        });
                                    debug!("{} tx: serial exec took {:?}", l, w.elapsed());

                                    nb_remaining_tx[scheduler] += backlog.len();
                                    if let Err(e) = worker_pool_outputs[scheduler].send(backlog).await {
                                        panic!("Failed to send transactions to scheduler");
                                    }

                                    backlog = to_execute;

                                    assert!(backlog.is_empty());

                                },
                                None => panic!("Scheduler dropped channel!")
                            }
                        } else {
                            // eprintln!(" has no tx to execute");
                        }
                    }
                    let remaining_tx = nb_remaining_tx.iter().fold(0, |a, b| a.add(b));
                    if remaining_tx + backlog.len() == 0 {
                        break 'process_batch;
                    }
                    round += 1;
                }

                // eprintln!();
                // eprintln!("Finished executing batch");
                if let Err(e) = send_result.send(()).await {
                    panic!("Failed to send result");
                }
            }

            eprintln!("Worker pool exiting");
            // println!("Execution took {:?}", duration);
        });

        let worker_pool = vec![coordinator];

        let scheduler_pool: Vec<_> = scheduler_inputs.into_iter()
            .zip(scheduler_outputs.into_iter())
            .enumerate()
            .map(|(scheduler_index, (mut input, output))| {
                tokio::spawn(async move {

                    let mut set_owned = AddressSet::with_capacity_and_max(chunk_size * max_addr_per_tx, storage_size as StaticAddress);
                    let mut backlog_owned: VecDeque<Transaction<A, P>> = VecDeque::with_capacity(2 * chunk_size);
                    let mut scheduled: Vec<Transaction<A, P>> = Vec::with_capacity(2 * chunk_size);

                    // TODO Scheduler is waiting for a new input to merge with its current backlog but the coordinator is waiting on this scheduler to get sth to execute
                    // => need to send them empty backlogs...
                    while let Some(mut to_schedule) = input.recv().await {
                        if to_schedule.is_empty() && backlog_owned.is_empty() {
                            // TODO Don't do anything !!!
                            // eprintln!("\t\t\t\t\tReturning vec to coordinator vec_pool");
                            if let Err(e) = output.send(to_schedule).await {
                                panic!("Unable to send vec {}", e);
                            }
                            continue;
                        }
                        // eprintln!("\t\t\t\tScheduler {} received {} tx to schedule", scheduler_index, to_schedule.len());
                        let backlog_len = backlog_owned.len();
                        // eprintln!("\t\t\t\t\tScheduling backlog of {} tx first", backlog_len);
                        'backlog_loop: for i in 0..backlog_len {
                            let tx = backlog_owned.pop_front().unwrap();

                            for addr in tx.addresses.iter() {
                                if !set_owned.insert(*addr) {
                                    /* TODO should make a set with the addresses of this tx and check intersection
                                        otherwise, a tx might conflict "with itself" if the same address
                                        appears multiple times in it parameters
                                     */

                                    // Tx can't be executed in this round
                                    backlog_owned.push_back(tx);
                                    continue 'backlog_loop;
                                }
                            }
                            // Tx can be executed this round
                            scheduled.push(tx);
                        }

                        // eprintln!("\t\t\t\tScheduling new transactions");
                        'to_schedule_loop: for tx in to_schedule.drain(..to_schedule.len()) {
                            for addr in tx.addresses.iter() {
                                if !set_owned.insert(*addr) {
                                    /* TODO Same as previous loop */
                                    // Tx can't be executed in this round
                                    backlog_owned.push_back(tx);
                                    continue 'to_schedule_loop;
                                }
                            }
                            // Tx can be executed this round
                            scheduled.push(tx);
                        }

                        assert!(to_schedule.is_empty());
                        assert!(!scheduled.is_empty());

                        // eprintln!("\t\t\t\t\tSending schedule of {} tx to coordinator", scheduled.len());
                        if let Err(e) = output.send(scheduled).await {
                            panic!("Unable to send schedule {}", e);
                        }

                        set_owned.clear();
                        // Reuse the capacity of to_schedule for the next iteration
                        scheduled = to_schedule;
                        // eprintln!("\t\t\t\tScheduler {} waiting on coordinator...", scheduler_index);
                    }

                    eprintln!("Scheduler {} exiting", scheduler_index);

                    ()
                })
            }).collect();

        let vm = Self{ storage, functions, send_batch, receive_result, nb_workers, worker_pool, nb_schedulers, scheduler_pool };
        return Ok(vm);
    }

    pub async fn execute(&mut self, mut batch: Vec<Transaction<A, P>>) -> anyhow::Result<()> {

        if let Err(e) = self.send_batch.send(batch).await {
            panic!("Failed to send batch to be executed");
        }

        if let None = self.receive_result.recv().await {
            panic!("Failed to receive result");
        }

        Ok(())
    }

    pub async fn stop(&mut self) {
        println!("Sending stop signal");
        if let Err(e) = self.send_batch.send(vec!()).await {
            panic!("Failed to send stop signal");
        }

        println!("Waiting on schedulers...");
        for scheduler in self.scheduler_pool.drain(..self.nb_schedulers) {
            if let Err(e) = scheduler.await {
                panic!("Failed to join scheduler");
            }
        }

        println!("Waiting on worker pool...");
        for pool in self.worker_pool.drain(..1) {
            if let Err(e) = pool.await {
                panic!("Failed to join worker pool");
            }
        }

        println!("Successfully stopped vm");
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

