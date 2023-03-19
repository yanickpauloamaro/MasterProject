#![allow(unused_variables)]

use std::cmp::max;
use crossbeam_utils::thread;
use std::collections::{HashMap, LinkedList, VecDeque};
use std::collections::hash_map::DefaultHasher;
use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};
use std::iter::Zip;
use std::mem;
use std::slice::Iter;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::{broadcast, oneshot, RwLock};
use tokio::time::{Duration, Instant};
use either::Either;
// use std::{mem, thread};
use std::sync::Arc;
use anyhow::{anyhow, Context, Error, Result};
use async_trait::async_trait;
use bloomfilter::Bloom;
use hwloc::Topology;
use tokio::task::JoinHandle;
use async_recursion::async_recursion;
use core_affinity::CoreId;
use tokio::task;
// use tokio::task;
use ed25519_dalek::Sha512;
use ed25519_dalek::Digest as _;

use crate::basic_vm::{BasicVM, BasicWorker};
use crate::transaction::{Instruction, Transaction, TransactionAddress, TransactionOutput};
use crate::config::Config;
use crate::{debug, debugging};
use crate::utils::{compatible, get_nb_nodes, print_metrics};
use crate::vm::{Batch, CHANNEL_CAPACITY, CPU, ExecutionResult, Jobs, VM, WorkerPool};
use crate::vm_implementation::{VMa, VMb, VMc, VmType, WorkerIndex};
use crate::worker_implementation::{WorkerB, WorkerBStd, WorkerBTokio};


// pub trait Assignment {
//
// }
// // pub struct
// pub struct None;
// pub struct Index {
//     i: WorkerIndex
// }
// pub struct Conflict;
// pub struct Executed;
//
// #[repr(i16)]
// pub enum Test {
//     // Executed = -2,
//     // Conflict = -1,
//     // None = 0,
//     // Index(u8),
//     // Executed(-2),
//     // Conflict(-1),
//     None(ZERO),
// }
// pub const ZERO: i16 = 0;
pub const EXECUTED_TEST: AssignedWorker = AssignedWorker::MAX-2;
pub const CONFLICT_TEST: AssignedWorker = AssignedWorker::MAX-1;
pub const NONE_TEST: AssignedWorker = 0;

pub const CONFLICT_WIP: u8 = u8::MAX;
pub const DONE: u8 = CONFLICT_WIP - 1;
pub const NONE_WIP: u8 = CONFLICT_WIP - 2;

//region test
pub type AssignedWorker = u16;

//endregion
pub trait Executor {
    fn execute(&mut self, mut backlog: Jobs) -> Result<Vec<ExecutionResult>> {
        todo!();
    }

    fn set_memory(&mut self, value: Word);
}

pub type Word = u64;
pub type Address = u64;

pub fn assign_workers_original(nb_workers: usize, batch: &Jobs, address_to_worker: &mut Vec<AssignedWorker>, backlog: &mut Jobs) -> Vec<AssignedWorker> {
    let mut tx_to_worker = vec![NONE_TEST; batch.len()];
    let mut next_worker = NONE_TEST;

    for (index, tx) in batch.iter().enumerate() {
        let from = tx.from as usize;
        let to = tx.to as usize;

        let worker_from = address_to_worker[from];
        let worker_to = address_to_worker[to];

        let assigned = match (worker_from, worker_to) {
            (NONE_TEST, NONE_TEST) => {
                // println!("Neither address is assigned: from={}, to={}", from, to);
                let worker = next_worker + 1;
                next_worker = (next_worker + 1) % nb_workers as AssignedWorker;
                address_to_worker[from] = worker;
                address_to_worker[to] = worker;
                worker
            },
            (worker, NONE_TEST) => {
                // println!("First address is assigned to {}: from={}, to={}", worker, from, to);
                address_to_worker[to] = worker;
                worker
            },
            (NONE_TEST, worker) => {
                // println!("Second address is assigned to {}: from={}, to={}", worker, from, to);
                address_to_worker[from] = worker;
                worker
            },
            (a, b) if a == b => {
                // println!("Both addresses are assigned to {}: from={}, to={}", a, from, to);
                a
            },
            (a, b) => {
                // println!("Both addresses are assigned to different workers: from={}->{}, to={}->{}", from, a, to, b);
                CONFLICT_TEST
            },
        };

        if assigned != CONFLICT_TEST {
            tx_to_worker[index] = assigned;
        } else {
            backlog.push(tx.clone());
        }
    }

    return tx_to_worker;
}

pub fn assign_workers_new_impl(nb_workers: usize, batch: &Jobs, address_to_worker: &mut Vec<AssignedWorker>, backlog: &mut Jobs) -> Vec<AssignedWorker> {
    let mut tx_to_worker = vec![NONE_TEST; batch.len()];
    let mut next_worker = 0 as AssignedWorker;

    for (index, tx) in batch.iter().enumerate() {
        let from = tx.from as usize;
        let to = tx.to as usize;

        let worker_from = address_to_worker[from];
        let worker_to = address_to_worker[to];

        let has_conflict = (worker_from != worker_to) &&
            (worker_from != NONE_TEST) &&
            (worker_to != NONE_TEST);

        if !has_conflict {
            let assign_next_worker = (worker_from == NONE_TEST) && (worker_to == NONE_TEST);

            let assigned_worker = max(worker_from, worker_from) +
                assign_next_worker as AssignedWorker * (next_worker + 1);

            address_to_worker[from] = assigned_worker;
            address_to_worker[to] = assigned_worker;
            tx_to_worker[index] = assigned_worker;

            next_worker = (next_worker + assign_next_worker as AssignedWorker) % nb_workers as AssignedWorker;

        } else {
            println!("***** Conflict between {} and {} for addresses {} and {}",
            worker_from, worker_to, tx.from, tx.to
            );
            backlog.push(tx.clone());
        }
    }

    return tx_to_worker;
}

pub fn assign_workers_new_impl_2(nb_workers: usize, batch: &Jobs, address_to_worker: &mut Vec<u8>, backlog: &mut Jobs,
s: &mut DefaultHasher) -> Vec<u8> {
    let mut tx_to_worker = vec![0; batch.len()];
    let mut next_worker = 0 as u8;

    for (index, tx) in batch.iter().enumerate() {
        // tx.from.hash(s);
        // let from = (s.finish() % address_to_worker.len() as u64) as usize;
        // tx.to.hash(s);
        // let to = (s.finish() % address_to_worker.len() as u64) as usize;

        // let from = (tx.from as usize) % address_to_worker.len(); // !!!
        // let to = (tx.to as usize) % address_to_worker.len(); // !!!

        let from = (tx.from as usize);
        let to = (tx.to as usize);

        let worker_from = address_to_worker[from];
        let worker_to = address_to_worker[to];

        let a = (worker_from != worker_to);
        let b = (worker_from != 0);
        let c = (worker_to != 0);
        let has_conflict = a && b && c;

        if !has_conflict {   // !!!
            let d = (worker_from == 0);
            let e = (worker_to == 0);
            let assign_next_worker = d && e;

            let f = max(worker_from, worker_from);
            let g = assign_next_worker as u8;
            let h = g * (next_worker + 1);
            let assigned_worker = f + h;

            address_to_worker[from] = assigned_worker;   // !!!
            address_to_worker[to] = assigned_worker; // !!!
            tx_to_worker[index] = assigned_worker;   // !!!

            let i = assign_next_worker as u8;
            let j = (next_worker + i);
            let k = nb_workers as u8;
            let l = j % k;
            next_worker = l;
        } else {
            backlog.push(tx.clone());
        }
    }

    return tx_to_worker;
}

pub fn assign_workers_dummy_modulo(nb_workers: usize, batch: &Jobs, address_to_worker: &mut Vec<AssignedWorker>, backlog: &mut Jobs) -> Vec<AssignedWorker> {
    let mut tx_to_worker = vec![NONE_TEST; batch.len()];
    let mut next_worker = 0 as AssignedWorker;

    for (index, tx) in batch.iter().enumerate() {
        let from = tx.from as usize;
        let to = tx.to as usize;

        let worker_from = address_to_worker[from];
        let worker_to = address_to_worker[to];

        let assigned = 1 + (index % 2) as AssignedWorker;

        if assigned != CONFLICT_TEST {
            tx_to_worker[index] = assigned;
        } else {
            backlog.push(tx.clone());
        }
    }

    return tx_to_worker;
}

pub fn assign_workers_dummy_modulo_u16(nb_workers: usize, batch: &Jobs, address_to_worker: &mut Vec<u16>, backlog: &mut Jobs) -> Vec<AssignedWorker> {
    let mut tx_to_worker = vec![u16::MAX; batch.len()];
    let mut next_worker = 0 as u16;

    for (index, tx) in batch.iter().enumerate() {
        let from = tx.from as usize;
        let to = tx.to as usize;

        let worker_from = address_to_worker[from];
        let worker_to = address_to_worker[to];

        let assigned = 1 + (index % 2) as u16;

        if assigned != CONFLICT_TEST {
            tx_to_worker[index] = assigned;
        } else {
            backlog.push(tx.clone());
        }
    }

    return tx_to_worker;
}


pub fn assign_workers(
    nb_workers: usize,
    batch: &Jobs,
    address_to_worker: &mut Vec<AssignedWorker>,
    backlog: &mut Jobs
) -> Vec<AssignedWorker> {
    let mut tx_to_worker = vec![NONE_TEST; batch.len()];
    let mut next_worker = 0 as AssignedWorker;

    for (index, tx) in batch.iter().enumerate() {
        let from = tx.from as usize;
        let to = tx.to as usize;

        let worker_from = address_to_worker[from];
        let worker_to = address_to_worker[to];

        let has_conflict = (worker_from != worker_to) &&
            (worker_from != NONE_TEST) &&
            (worker_to != NONE_TEST);

        if !has_conflict {
            // A, None
            // None, B
            // None, None
            // X, X

            let assign_next_worker = (worker_from == NONE_TEST) && (worker_to == NONE_TEST);

            let assigned_worker = max(worker_from, worker_from) +
                assign_next_worker as AssignedWorker * (next_worker + 1);

            address_to_worker[from] = assigned_worker;
            address_to_worker[to] = assigned_worker;
            tx_to_worker[index] = assigned_worker;

            next_worker = (next_worker + assign_next_worker as AssignedWorker) % nb_workers as AssignedWorker;

        } else {
            backlog.push(tx.clone());
        }

        // let assigned = match (worker_from, worker_to) {
        //     (NONE_TEST, NONE_TEST) => {
        //         // println!("Neither address is assigned: from={}, to={}", from, to);
        //         let worker = next_worker + 1;
        //         next_worker = (next_worker + 1) % nb_workers as i16;
        //         address_to_worker[from] = worker;
        //         address_to_worker[to] = worker;
        //         worker
        //     },
        //     (worker, NONE_TEST) => {
        //         // println!("First address is assigned to {}: from={}, to={}", worker, from, to);
        //         address_to_worker[to] = worker;
        //         worker
        //     },
        //     (NONE_TEST, worker) => {
        //         // println!("Second address is assigned to {}: from={}, to={}", worker, from, to);
        //         address_to_worker[from] = worker;
        //         worker
        //     },
        //     (a, b) if a == b => {
        //         // println!("Both addresses are assigned to {}: from={}, to={}", a, from, to);
        //         a
        //     },
        //     (a, b) => {
        //         // println!("Both addresses are assigned to different workers: from={}->{}, to={}->{}", from, a, to, b);
        //         CONFLICT_TEST
        //     },
        // };
        let assigned = 1 + (index % 2) as AssignedWorker;
        // let assigned = 1;

        if assigned == CONFLICT_TEST {
            backlog.push(tx.clone());
        } else {
            tx_to_worker[index] = assigned;
        }

        // if assigned != CONFLICT_TEST {
        //     tx_to_worker[index] = assigned;
        // } else {
        //     backlog.push(tx.clone());
        // }
    }

    return tx_to_worker;
}

// =================================================================================================
struct BloomFilter {
    filter: Bloom<TransactionAddress>
}

impl BloomFilter {
    pub fn new(expected_size: usize, false_positive: f64) -> Self {
        Self{ filter: Bloom::new_for_fp_rate(expected_size, false_positive) }
    }
    pub fn has(&self, tx: &Transaction) -> bool {
        return self.filter.check(&tx.from) || self.filter.check(&tx.to)
    }

    pub fn insert(&mut self, tx: &Transaction) {
        self.filter.set(&tx.from);
        self.filter.set(&tx.to);
    }

    pub fn clear(&mut self) {
        self.filter.clear();
    }
}

pub struct BloomVM {
    nb_workers: usize,
    batch_size: usize,
    filters: Vec<BloomFilter>,
    tx_jobs: Vec<Sender<Jobs>>,
    rx_results: Receiver<(Vec<ExecutionResult>, Jobs)>
}

impl BloomVM {
    // TODO Is there a way to call the original trait implementation?
    async fn execute(&mut self, mut backlog: Jobs) -> Result<Vec<ExecutionResult>> {
        debug!("Clearing bloom filters");
        self.clear_filters();
        return VM::execute(self, backlog).await;
    }

    fn clear_filters(&mut self) {
        for filter in self.filters.iter_mut() {
            filter.clear();
        }
    }

    pub async fn init(&mut self, jobs: Jobs) -> Result<Vec<ExecutionResult>>{
        println!("Sending init to workers");
        for sender in self.tx_jobs.iter() {
            sender.send(jobs.clone()).await;
        }
        println!("Collecting init");
        let res = self.collect().await?;
        return Ok(res.0);
    }
}

#[async_trait]
impl VM for BloomVM {
    fn new(nb_workers: usize, batch_size: usize) -> Self {
        let false_positive: f64 = 0.1;  // 10%?
        let (tx_jobs, rx_results) = BasicWorker::new_worker_pool(nb_workers);
        let filters = (0..nb_workers).map(|_| BloomFilter::new(batch_size, false_positive)).collect();

        return Self {
            nb_workers,
            batch_size,
            filters,
            tx_jobs,
            rx_results
        };
    }

    async fn prepare(&mut self) {
        println!("Waiting for workers to be ready (2s)");
        // TODO Implement a real way of knowing when the workers are ready...
        tokio::time::sleep(Duration::from_secs(2)).await;
    }

    async fn dispatch(&mut self, backlog: &mut Jobs) -> Result<Jobs> {
        debug!("Dispatch =======================");
        // TODO once we have mini transactions, we should not reset the filters in dispatch, only in execute
        // TODO Maybe we can partially clear the Bloom ? i.e. only clear the receivers
        self.clear_filters();

// let start = Instant::now();
// let mut finding_assigned_workers = Duration::from_micros(0);
// let mut rest = Duration::from_micros(0);
        let mut worker_jobs: Vec<Jobs> = (0..self.nb_workers).map(|_| vec!()).collect();
        let mut conflicting_jobs: Jobs = vec!();
        let mut next_worker = 0;
        for tx in backlog.drain(..backlog.len()) {
            // Find which worker is responsible for the addresses in this transaction
// let mut a = Instant::now();
            let mut assigned_workers = vec!();
            for w in 0..self.nb_workers {
                if self.filters[w].has(&tx) {
                    assigned_workers.push(w);
                }
            }
// finding_assigned_workers += a.elapsed();
// a = Instant::now();
            let assignee = match assigned_workers[..] {
                [] => {
                    // No worker is responsible for this tx, so pick the next one
                    debug!("{:?} -> Assigned to {}", tx, next_worker);
                    let worker = next_worker;
                    next_worker = (next_worker + 1) % self.nb_workers;
                    worker
                },
                [worker] => {
                    // A worker is responsible for this tx
                    debug!("{:?} -> Claimed by {}", tx, worker);
                    worker
                },
                _ => {
                    // There might be a conflict among workers
                    // Possible reasons:
                    //  - the bloom filter returned a false positive for some worker(s)
                    //  - the tx accesses addresses that are from assigned to different workers
                    debug!("{:?} -> Conflict between {:?}", tx, assigned_workers);
                    conflicting_jobs.push(tx);
                    continue;
                }
            };

            // let assignee = assigned_workers.pop().unwrap_or_else(|| {
            //     debug!("{:?} -> Assigned to {}", tx, next_worker);
            //     let worker = next_worker;
            //     next_worker = (next_worker + 1) % self.nb_workers;
            //     worker
            // });
            //
            // if assigned_workers.len() > 1 {
            //     debug!("{:?} -> Conflict between {:?}", tx, assigned_workers);
            //     conflicting_jobs.push(tx);
            //     continue;
            // }

            // Assign the transaction to the worker
            self.filters[assignee].insert(&tx);
            worker_jobs[assignee].push(tx);
// rest += a.elapsed();
        }
// println!("Separating transactions took {:?}", start.elapsed());
// println!("\tIncluding {:?} for finding assigned workers", finding_assigned_workers);
// println!("\tIncluding {:?} for the rest", rest);
// println!();

        // Send jobs to each worker
        debug!("Sending jobs to workers...");
// let start = Instant::now();
        for (worker, jobs) in worker_jobs.into_iter().enumerate() {
            // N.B: Must send to worker even if the jobs contains no transaction because ::collect
            // expects all workers to send a result
            self.tx_jobs[worker].send(jobs).await
                .context(format!("Unable to send job to worker"))?;
        }
// println!("Sending transactions took {:?}", start.elapsed());
// println!();

        debug!();

        Ok(conflicting_jobs)
    }

    async fn collect(&mut self) -> Result<(Vec<ExecutionResult>, Jobs)> {

        let mut collected_results = Vec::with_capacity(self.batch_size);
        let mut collected_jobs = Vec::with_capacity(self.batch_size);

        debug!("Collecting responses...");
        // Wait for all workers to give a response
        for _ in 0..self.nb_workers {
            let (mut results, mut jobs) = self.rx_results.recv().await
                .ok_or(anyhow::anyhow!("Unable to receive results from workers"))?;

            collected_results.append(&mut results);
            collected_jobs.append(&mut jobs);
        }
        debug!("Got all responses");
        debug!();

        return Ok((collected_results, collected_jobs));
    }
}


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
