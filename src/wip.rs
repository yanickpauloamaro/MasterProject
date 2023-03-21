#![allow(unused_variables)]

use std::cmp::max;
use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;

use hwloc::Topology;

use crate::transaction::TransactionAddress;
use crate::utils::compatible;
use crate::vm::Jobs;
use crate::vm_utils::{CONFLICTING, UNASSIGNED};

pub const CONFLICT_WIP: u8 = u8::MAX;
pub const DONE: u8 = CONFLICT_WIP - 1;
pub const NONE_WIP: u8 = CONFLICT_WIP - 2;

pub type AssignedWorker = u8;
pub type Word = u64;
pub type Address = u64;

pub fn assign_workers_original(nb_workers: usize, batch: &Jobs, address_to_worker: &mut Vec<AssignedWorker>, backlog: &mut Jobs) -> Vec<AssignedWorker> {
    let mut tx_to_worker = vec![UNASSIGNED; batch.len()];
    let mut next_worker = UNASSIGNED;

    for (index, tx) in batch.iter().enumerate() {
        let from = tx.from as usize;
        let to = tx.to as usize;

        let worker_from = address_to_worker[from];
        let worker_to = address_to_worker[to];

        let assignment = match (worker_from, worker_to) {
            (UNASSIGNED, UNASSIGNED) => {
                // println!("Neither address is assigned: from={}, to={}", from, to);
                let worker = next_worker + 1;
                next_worker = (next_worker + 1) % nb_workers as AssignedWorker;
                address_to_worker[from] = worker;
                address_to_worker[to] = worker;
                worker
            },
            (worker, UNASSIGNED) => {
                // println!("First address is assigned to {}: from={}, to={}", worker, from, to);
                address_to_worker[to] = worker;
                worker
            },
            (UNASSIGNED, worker) => {
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
                CONFLICTING
            },
        };

        if assignment != CONFLICTING {
            tx_to_worker[index] = assignment;
        } else {
            backlog.push(tx.clone());
        }
    }

    return tx_to_worker;
}

pub fn assign_workers_new_impl(nb_workers: usize, batch: &Jobs, address_to_worker: &mut Vec<AssignedWorker>, backlog: &mut Jobs) -> Vec<AssignedWorker> {
    let mut tx_to_worker = vec![UNASSIGNED; batch.len()];
    let mut next_worker = 0 as AssignedWorker;

    for (index, tx) in batch.iter().enumerate() {
        let from = tx.from as usize;
        let to = tx.to as usize;

        let worker_from = address_to_worker[from];
        let worker_to = address_to_worker[to];

        let has_conflict = (worker_from != worker_to) &&
            (worker_from != UNASSIGNED) &&
            (worker_to != UNASSIGNED);

        if !has_conflict {
            let assign_next_worker = (worker_from == UNASSIGNED) && (worker_to == UNASSIGNED);

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

        let from = tx.from as usize;
        let to = tx.to as usize;

        let worker_from = address_to_worker[from];
        let worker_to = address_to_worker[to];

        let a = worker_from != worker_to;
        let b = worker_from != 0;
        let c = worker_to != 0;
        let has_conflict = a && b && c;

        if !has_conflict {   // !!!
            let d = worker_from == 0;
            let e = worker_to == 0;
            let assign_next_worker = d && e;

            let f = max(worker_from, worker_from);
            let g = assign_next_worker as u8;
            let h = g * next_worker + 1;
            let assigned_worker = f + h;

            address_to_worker[from] = assigned_worker;   // !!!
            address_to_worker[to] = assigned_worker; // !!!
            tx_to_worker[index] = assigned_worker;   // !!!

            let i = assign_next_worker as u8;
            let j = next_worker + i;
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
    let mut tx_to_worker = vec![UNASSIGNED; batch.len()];
    let mut next_worker = UNASSIGNED;

    for (index, tx) in batch.iter().enumerate() {
        let from = tx.from as usize;
        let to = tx.to as usize;

        let worker_from = address_to_worker[from];
        let worker_to = address_to_worker[to];

        let assigned = match (worker_from, worker_to) {
            (UNASSIGNED, UNASSIGNED) => {
                // println!("Neither address is assigned: from={}, to={}", from, to);
                let worker = next_worker + 1;
                next_worker = (next_worker + 1) % nb_workers as AssignedWorker;
                address_to_worker[from] = worker;
                address_to_worker[to] = worker;
                worker
            },
            (worker, UNASSIGNED) => {
                // println!("First address is assigned to {}: from={}, to={}", worker, from, to);
                address_to_worker[to] = worker;
                worker
            },
            (UNASSIGNED, worker) => {
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
    let mut tx_to_worker = vec![UNASSIGNED; batch.len()];
    let mut next_worker = UNASSIGNED;

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
        if worker_from == UNASSIGNED && worker_to == UNASSIGNED {
            let assigned = next_worker + 1;
            address_to_worker[from] = assigned;
            address_to_worker[to] = assigned;
            tx_to_worker[index] = assigned;
            next_worker = (next_worker + 1) % nb_workers as AssignedWorker;

        } else if worker_from == UNASSIGNED || worker_to == UNASSIGNED {
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
    let mut tx_to_worker = vec![UNASSIGNED; batch.len()];
    let mut next_worker = UNASSIGNED;

    for (index, tx) in batch.iter().enumerate() {
        let from = tx.from;
        let to = tx.to;

        // let from = (tx.from as usize) % address_to_worker.len(); // !!!
        // let to = (tx.to as usize) % address_to_worker.len(); // !!!

        // let from = (tx.from as usize) >> 1; // !!!
        // let to = (tx.to as usize) >> 1; // !!!

        let worker_from = address_to_worker.get(&from)
            .unwrap_or(&UNASSIGNED);
        let worker_to = address_to_worker.get(&to)
            .unwrap_or(&UNASSIGNED);

        // Slightly better
        if *worker_from == UNASSIGNED && *worker_to == UNASSIGNED {
            let assigned = next_worker + 1;
            address_to_worker.insert(from, assigned);
            address_to_worker.insert(to, assigned);
            tx_to_worker[index] = assigned;
            next_worker = (next_worker + 1) % nb_workers as AssignedWorker;

        } else if *worker_from == UNASSIGNED || *worker_to == UNASSIGNED {
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
    let mut tx_to_worker = vec![UNASSIGNED; batch.len()];
    // let mut next_worker = 0 as AssignedWorker;

    for (index, tx) in batch.iter().enumerate() {
        let from = tx.from as usize;
        let to = tx.to as usize;

        let worker_from = address_to_worker[from];
        let worker_to = address_to_worker[to];

        let assigned = index as AssignedWorker;

        if assigned != CONFLICTING {
            tx_to_worker[index] = assigned;
        } else {
            backlog.push(tx.clone());
        }
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
