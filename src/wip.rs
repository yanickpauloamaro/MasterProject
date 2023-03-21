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

use crate::transaction::{Instruction, Transaction, TransactionAddress, TransactionOutput};
use crate::{debug, debugging};
use crate::utils::{compatible, get_nb_nodes};
use crate::vm::{Batch, CPU, ExecutionResult, Jobs};
use crate::vm_implementation::{VMa, VMb, VMc, VmType};
use crate::worker_implementation::{WorkerB, WorkerBStd, WorkerBTokio};

pub const EXECUTED_TEST: AssignedWorker = AssignedWorker::MAX-2;
pub const CONFLICT_TEST: AssignedWorker = AssignedWorker::MAX-1;
pub const NONE_TEST: AssignedWorker = 0;

pub const CONFLICT_WIP: u8 = u8::MAX;
pub const DONE: u8 = CONFLICT_WIP - 1;
pub const NONE_WIP: u8 = CONFLICT_WIP - 2;

pub type AssignedWorker = u8;
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

pub fn assign_workers_new_impl_3(nb_workers: usize, batch: &Jobs, address_to_worker: &mut Vec<AssignedWorker>, backlog: &mut Jobs) -> Vec<AssignedWorker> {
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
                backlog.push(tx.clone());
                continue;
            },
        };

        tx_to_worker[index] = assigned;
    }

    return tx_to_worker;
}

pub fn assign_workers_new_impl_4(nb_workers: usize, batch: &Jobs, address_to_worker: &mut Vec<AssignedWorker>, backlog: &mut Jobs) -> Vec<AssignedWorker> {
    let mut tx_to_worker = vec![NONE_TEST; batch.len()];
    let mut next_worker = NONE_TEST;

    for (index, tx) in batch.iter().enumerate() {

        // let from = (tx.from as usize) % address_to_worker.len(); // !!!
        // let to = (tx.to as usize) % address_to_worker.len(); // !!!

        let from = tx.from as usize;
        let to = tx.to as usize;
        // let from = (tx.from as usize) >> 1; // !!!
        // let to = (tx.to as usize) >> 1; // !!!

        let worker_from = address_to_worker[from];
        let worker_to = address_to_worker[to];

        // if worker_from == NONE_TEST && worker_to == NONE_TEST {
        //     let assigned = next_worker + 1;
        //     address_to_worker[from] = assigned;
        //     address_to_worker[to] = assigned;
        //     tx_to_worker[index] = assigned;
        //     next_worker = (next_worker + 1) % nb_workers as AssignedWorker;
        //
        // } else if worker_from == NONE_TEST {
        //     address_to_worker[from] = worker_to;
        //     tx_to_worker[index] = worker_to;
        //
        // } else if worker_to == NONE_TEST {
        //     address_to_worker[to] = worker_from;
        //     tx_to_worker[index] = worker_from;
        //
        // } else if worker_from == worker_to {
        //     tx_to_worker[index] = worker_from;
        //
        // } else {
        //     backlog.push(tx.clone());
        // }

        // Slightly better
        if worker_from == NONE_TEST && worker_to == NONE_TEST {
            let assigned = next_worker + 1;
            address_to_worker[from] = assigned;
            address_to_worker[to] = assigned;
            tx_to_worker[index] = assigned;
            next_worker = (next_worker + 1) % nb_workers as AssignedWorker;

        } else if worker_from == NONE_TEST || worker_to == NONE_TEST {
            let assigned = max(worker_from, worker_to);
            address_to_worker[from] = assigned;
            address_to_worker[to] = assigned;
            tx_to_worker[index] = assigned;

        } else if worker_from == worker_to {
            tx_to_worker[index] = worker_from;

        } else {
            // println!("** conflict:");
            // println!("addr {} -> worker {}", from, worker_from);
            // println!("addr {} -> worker {}", to, worker_to);
            backlog.push(tx.clone());
        }
    }

    return tx_to_worker;
}

pub fn assign_workers_new_impl_5<'a>(nb_workers: usize, batch: &'a Jobs, address_to_worker: &mut HashMap<TransactionAddress, AssignedWorker>, backlog: &mut Jobs) -> Vec<AssignedWorker> {
    let mut tx_to_worker = vec![NONE_TEST; batch.len()];
    let mut next_worker = NONE_TEST;

    for (index, tx) in batch.iter().enumerate() {
        let from = tx.from;
        let to = tx.to;

        // let from = (tx.from as usize) % address_to_worker.len(); // !!!
        // let to = (tx.to as usize) % address_to_worker.len(); // !!!

        // let from = (tx.from as usize) >> 1; // !!!
        // let to = (tx.to as usize) >> 1; // !!!

        let worker_from = address_to_worker.get(&from)
            .unwrap_or(&NONE_TEST);
        let worker_to = address_to_worker.get(&to)
            .unwrap_or(&NONE_TEST);

        // Slightly better
        if *worker_from == NONE_TEST && *worker_to == NONE_TEST {
            let assigned = next_worker + 1;
            address_to_worker.insert(from, assigned);
            address_to_worker.insert(to, assigned);
            tx_to_worker[index] = assigned;
            next_worker = (next_worker + 1) % nb_workers as AssignedWorker;

        } else if *worker_from == NONE_TEST || *worker_to == NONE_TEST {
            let assigned = max(*worker_from, *worker_to);
            address_to_worker.insert(from, assigned);
            address_to_worker.insert(to, assigned);
            tx_to_worker[index] = assigned;

        } else if *worker_from == *worker_to {
            tx_to_worker[index] = *worker_from;

        } else {
            backlog.push(tx.clone());
        }
    }

    return tx_to_worker;
}

pub fn assign_workers_dummy_modulo(
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

        let assigned = index as AssignedWorker;

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
