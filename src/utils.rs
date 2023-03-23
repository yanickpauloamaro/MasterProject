use std::collections::HashMap;
use std::ops::{Div, Mul};
use std::time::Duration;

use anyhow::Result;
use hwloc::{ObjectType, Topology};
use rand::rngs::StdRng;
use rand::seq::{IteratorRandom, SliceRandom};

use crate::transaction::{Instruction, Transaction, TransactionAddress};
use crate::vm::Jobs;
use crate::wip::Word;

#[macro_export]
macro_rules! debugging {
    () => {
        false
    };
}

#[macro_export]
macro_rules! debug {
    () => {
        if debugging!() {
            println!();
        }
    };
    ($($arg:tt)*) => {{
        if debugging!() {
            // print!("**");
            println!($($arg)*);
        }
    }};
}

pub fn check(condition: bool, ctx: &str) -> Result<()> {

    if condition {
        Ok(())
    } else {
        let error_msg = anyhow::anyhow!("Host not compatible. {} not supported", ctx);
        Err(error_msg)
    }
}

pub fn compatible(topo: &Topology) -> Result<()> {

    check(topo.support().cpu().set_current_process(), "CPU Binding (current process)")?;
    check(topo.support().cpu().set_process(), "CPU Binding (any process)")?;
    check(topo.support().cpu().set_current_thread(), "CPU Binding (current thread)")?;
    check(topo.support().cpu().set_thread(), "CPU Binding (any thread)")?;

    Ok(())
}

pub fn get_nb_cores(topo: &Topology) -> usize {
    // TODO use num_cpus crate
    let core_depth = topo.depth_or_below_for_type(&ObjectType::Core).unwrap();
    let all_cores = topo.objects_at_depth(core_depth);
    return all_cores.len();
}

pub fn get_nb_nodes(topo: &Topology, requested_nb_nodes: Option<usize>) -> Result<usize> {
    let nb_cores = get_nb_cores(&topo);
    match requested_nb_nodes {
        Some(nb_nodes) if nb_nodes > nb_cores => {
            let error_msg = anyhow::anyhow!(
                "Not enough cores. {} requested but only {} available",
                nb_nodes, nb_cores);
            return Err(error_msg);
        },
        Some(nb_nodes) => Ok(nb_nodes),
        None => Ok(nb_cores)
    }
}

pub fn batch_account_creation(batch_size: usize, nb_accounts: usize, amount: u64) -> Vec<Transaction> {
    let mut batch = Vec::with_capacity(batch_size);
    for i in 0..nb_accounts {
        let create = Instruction::CreateAccount(i as u64, amount as Word);
        let tx = Transaction {
            from: 0,
            to: 0,
            instructions: vec!(create),
            // parameters: vec!()
        };
        batch.push(tx);
    }

    return batch;
}

fn transfer(from: TransactionAddress, to: TransactionAddress, amount: u64) -> Vec<Instruction> {
    return vec!(
        Instruction::Decrement(from, amount as Word),
        Instruction::Increment(to, amount as Word),
    );
}

pub fn batch_transfer_loop(batch_size: usize, nb_account: usize) -> Vec<Transaction> {

    let mut batch = Vec::with_capacity(batch_size);
    for account in 0..(nb_account-1) {
        let i = account as u64;
        let next = (i + 1) % nb_account as u64;
        let tx = Transaction {
            from: account as u64,
            to: next,
            instructions: transfer(i, next, i),
            // parameters: vec!()
        };
        batch.push(tx);
    }

    // TODO If we shuffle the batch, we probably get different performance
    return batch;
}

pub fn batch_with_conflicts(batch_size: usize, conflict_rate: f64, mut rng: &mut StdRng) -> Jobs {

    let nb_conflict = (conflict_rate * batch_size as f64).ceil() as usize;

    let mut receiver_occurrences: HashMap<u64, u64> = HashMap::new();
    let mut batch = Vec::with_capacity(batch_size);

    // Create non-conflicting transactions
    for i in 0..batch_size {
        let amount = 2;
        let from = i as u64;
        let mut to = i as u64;

        // Ensure senders and receivers don't conflict. Otherwise, would need to count conflicts
        // between senders and receivers
        to += batch_size as u64;

        receiver_occurrences.insert(to, 1);

        let instructions = transfer(from, to, amount);
        let tx = Transaction{ from, to, instructions};
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

pub fn batch_with_conflicts_new_impl(memory_size: usize, batch_size: usize, conflict_rate: f64, mut rng: &mut StdRng) -> Jobs {

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

        let instructions = transfer(from, to, amount);
        let tx = Transaction{ from, to, instructions};
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

pub fn print_conflict_rate(batch: &Vec<Transaction>) {

    let mut nb_addresses = 0;

    let mut conflicts = HashMap::new();
    let mut nb_conflicts = 0;
    let mut nb_conflicting_addr = 0;

    for tx in batch.iter() {
        // The 'from' address is always different
        nb_addresses += 1;

        // TODO Make this computation work for arbitrary transfer graphs
        // if addresses.insert(tx.from) {
        //     nb_addresses += 1;
        // }
        // if addresses.insert(tx.to) {
        //     nb_addresses += 1;
        // }

        match conflicts.get_mut(&tx.to) {
            None => {
                conflicts.insert(tx.to, 1);
                nb_addresses += 1;
            },
            Some(occurrence) if *occurrence == 1 => {
                *occurrence += 1;
                nb_conflicts += 2;
                nb_conflicting_addr += 1;
                // println!("** {} is appearing for the 2nd time", tx.to);

            },
            Some(occurrence) => {
                *occurrence += 1;
                nb_conflicts += 1;
                // println!("** {} is appearing for the {}-th time", tx.to, *occurrence);
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

pub fn batch_partitioned(batch_size: usize, nb_partitions: usize) -> Vec<Transaction> {
    let mut batch = Vec::with_capacity(batch_size);
    let chunks = (batch_size / nb_partitions) as u64;

    for i in 0..batch_size {
        let from = i as u64 / chunks;
        let to = ((nb_partitions + i) % (2 * nb_partitions)) as u64;
        let amount = 2 as u64;
        let tx = Transaction{
            from,
            to,
            instructions: transfer(from, to, amount),
        };

        batch.push(tx);
    }

    return batch;
}

pub fn print_throughput(nb_batches: usize, nb_transactions: usize, duration: Duration) {
    println!("Processed {} batches = {} txs in {:?}",
             nb_batches, nb_transactions, duration);

    let micro_throughput = (nb_transactions as f64).div(duration.as_micros() as f64);
    let milli_throughput = micro_throughput.mul(1000.0);
    let throughput = milli_throughput.mul(1000.0);

    println!("Throughput is {} tx/µs", micro_throughput);
    println!("Throughput is {} tx/ms", milli_throughput);
    println!("Throughput is {} tx/s", throughput);
}