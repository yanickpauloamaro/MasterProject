use std::mem;
use std::thread::park_timeout;
use std::time::Duration;
use async_trait::async_trait;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::time::Instant;
use anyhow::{self, Context, Error, Result};
use hwloc::Topology;
use tokio::task::JoinHandle;
use basic_vm::BasicVM;

use crate::transaction::Transaction;
use crate::vm::{ExecutionResult, VM};
use crate::config::Config;
use crate::transaction::TransactionOutput;
// use crate::vm::BasicVM;
use crate::basic_vm::BasicWorker;
// use crate::wip::Worker;

pub mod benchmark;
mod transaction;
pub mod config;
mod wip;
pub mod vm;
pub mod basic_vm;

pub fn create_batch(batch_size: usize) {
    // const NB_ITER: u32 = 100;
    let mut batch = Vec::with_capacity(batch_size);
    for _ in 0..batch_size {
        let tx = transaction::Transaction {
            from: 0,
            to: 1,
            amount: 42,
        };
        batch.push(tx);
    }
}

fn check(condition: bool, ctx: &str) -> Result<()> {

    if condition {
        Ok(())
    } else {
        let error_msg = anyhow::anyhow!("Host not compatible. {} not supported", ctx);
        Err(error_msg)
    }
}

fn compatible(topo: &Topology) -> Result<()> {

    check(topo.support().cpu().set_current_process(), "CPU Binding (current process)")?;
    check(topo.support().cpu().set_process(), "CPU Binding (any process)")?;
    check(topo.support().cpu().set_current_thread(), "CPU Binding (current thread)")?;
    check(topo.support().cpu().set_thread(), "CPU Binding (any thread)")?;

    Ok(())
}

pub async fn benchmark_vm(config: Config) -> Result<()> {

    // Setup ---------------------------------------------------------------------------------------
    const CHANNEL_CAPACITY: usize = 200;

    let topo = Topology::new();

    // Check compatibility with core pinning
    compatible(&topo)?;

    let nb_nodes = wip::get_nb_nodes(&topo, &config)?;
    let mut vm = BasicVM::new(nb_nodes);

    // Benchmark -----------------------------------------------------------------------------------
    let mut batch = Vec::with_capacity(config.batch_size);
    let mut address: u64 = 0;
    let chunks = (config.batch_size / nb_nodes) as u64;

    for i in 0..config.batch_size {
        let tx = Transaction{
            from: address / chunks,
            to: nb_nodes as u64,
            amount: i as u64,
        };

        // println!("tx: {:?}", tx);
        batch.push(tx);
        address += 1;
    }
    let (results, execution_start, duration) = vm.execute(batch).await
        .context("Failed to execute batch")?;

    // Computing latency ---------------------------------------------------------------------------
    let mut processed: u64 = 0;
    let mut sum_latency = Duration::from_millis(0);
    let mut min_latency = Duration::MAX;
    let mut max_latency = Duration::from_nanos(0);

    for result in results {
        let latency = result.execution_end - execution_start;
        sum_latency += latency;
        min_latency = min_latency.min(latency);
        max_latency = max_latency.max(latency);
        processed += 1;
    }

    println!("Batch size: {}, tx processed: {}", config.batch_size, processed);
    println!("Executed {} tx in {:?}", config.batch_size, duration);

    let micro_throughput = config.batch_size as f64 / duration.as_micros() as f64;
    let milli_throughput = 1000.0 * micro_throughput;
    let throughput = 1000.0 * milli_throughput;

    println!("Throughput is {} tx/µs", micro_throughput);
    println!("Throughput is {} tx/ms", milli_throughput);
    // println!("Throughput is {} tx/s", throughput);
    println!();
    println!("Min latency is {:?}", min_latency);
    println!("Max latency is {:?}", max_latency);
    println!("Average latency is {:?} µs", sum_latency.as_micros() / processed as u128);
    println!("Average latency is {:?} ms", sum_latency.as_millis() / processed as u128);

    Ok(())
}