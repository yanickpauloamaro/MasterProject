use std::ops::{Add, Div, Mul};
use std::time::Duration;
use anyhow::{anyhow, Result};
use hwloc::{ObjectType, Topology, TopologyObject};
use num_traits::{Inv, ToPrimitive, Zero};
use tokio::time::Instant;
use rand::seq::SliceRandom;

use crate::config::Config;
use crate::transaction::{Instruction, Transaction, TransactionAddress};
use crate::vm::{ExecutionResult, Jobs};

// const DEBUG: bool = false;
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
            print!("**");
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
    let core_depth = topo.depth_or_below_for_type(&ObjectType::Core).unwrap();
    let all_cores = topo.objects_at_depth(core_depth);
    return all_cores.len();
}

pub fn get_nb_nodes(topo: &Topology, config: &Config) -> Result<usize> {
    let nb_cores = get_nb_cores(&topo);
    match config.nb_nodes {
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

pub fn account_creation_batch(batch_size: usize, nb_accounts: usize, amount: u64) -> Vec<Transaction> {
    let mut batch = Vec::with_capacity(batch_size);
    for i in 0..nb_accounts {
        let create = Instruction::CreateAccount(i as u64, amount);
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

pub fn transfer(from: TransactionAddress, to: TransactionAddress, amount: u64) -> Vec<Instruction> {
    return vec!(
        Instruction::Decrement(from, amount),
        Instruction::Increment(to, amount),
    );
}

pub fn transaction_loop(batch_size: usize, nb_account: usize) -> Vec<Transaction> {

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

    return batch;
}

pub trait AddressGenerator {
    fn next(&mut self) -> TransactionAddress;
}

pub struct ContentionGenerator {
    from_address: TransactionAddress,
    contention_accounts: Vec<TransactionAddress>
}

impl ContentionGenerator {
    pub fn new(from_address: TransactionAddress, contention_rate: f64) -> Result<Self> {

        let contention_accounts = if contention_rate.is_zero() {
            vec!()
        } else {
            let nb_contention_accounts = contention_rate.inv().ceil()
                .to_usize()
                .ok_or(anyhow!("Unable to compute number of contention accounts"))?;

            (0..nb_contention_accounts)
                .map(|addr| from_address + addr as u64)
                .map(|addr| addr as u64)
                .collect()
        };

        return Ok(Self {
            from_address,
            contention_accounts
        })
    }

    pub fn nb_contention_accounts(&self) -> usize {
        if self.contention_accounts.is_empty() {
            return self.from_address as usize;
        }
        return self.contention_accounts.len();
    }
}

impl AddressGenerator for ContentionGenerator {
    fn next(&mut self) -> TransactionAddress {
        match self.contention_accounts.choose(&mut rand::thread_rng()) {
            Some(addr) => *addr,
            None => {
                let addr = self.from_address;
                self.from_address += 1;
                addr
            }
        }
    }
}

pub fn create_batch_from_generator<G>(batch_size: usize, generator: &mut G) -> Jobs
where G: AddressGenerator {

    let mut batch = Vec::with_capacity(batch_size);

    for addr in 0..batch_size {
        let from = addr as u64;
        let to = generator.next() ;
        let amount = 2;

        let tx = Transaction{
            from,
            to,
            instructions: transfer(from, to, amount),
        };

        batch.push(tx);
    }

    return batch;
}

// pub fn create_conflict__batch(batch_size: usize, seed: usize, conflict_chance: f64) {
//     let nb_conlfict_accounts = conflict_chance.inv().ceil().to_usize();
// }

pub fn create_batch_partitioned(batch_size: usize, nb_partitions: usize) -> Vec<Transaction> {
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

pub fn print_metrics(
    batch_results: Vec<(Vec<ExecutionResult>, Instant, Duration)>,
    total_duration: Duration
) {

    // let mut processed: u64 = 0;
    // let nb_batches = batch_results.len();
    // let mut sum_latency = Duration::from_millis(0);
    // let mut min_latency = Duration::MAX;
    // let mut max_latency = Duration::from_nanos(0);

    // for (results, execution_start, _duration) in batch_results {
    //     for result in results {
    //         let latency = result.execution_end - execution_start;
    //         sum_latency += latency;
    //         min_latency = min_latency.min(latency);
    //         max_latency = max_latency.max(latency);
    //         processed += 1;
    //     }
    // }

    // println!("Batch size: {}, tx processed: {}", batch_size, processed);
    // print_throughput(nb_batches, processed as usize, total_duration);
    // println!();
    // println!("Min latency is {:?}", min_latency);
    // println!("Max latency is {:?}", max_latency);
    // // println!("Average latency is {:?} µs", sum_latency.as_micros() / processed as u128);
    // println!("Average latency is {:?} ms", sum_latency.as_millis() / processed as u128);
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