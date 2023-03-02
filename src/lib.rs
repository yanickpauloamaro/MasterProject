use std::mem;
use std::time::Duration;
use async_trait::async_trait;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::time::Instant;
use anyhow::{self, Result, Context, Error};
use hwloc::Topology;
use tokio::task::JoinHandle;
use crate::config::Config;
use crate::wip::{Transaction, Worker};

mod client;
pub mod benchmark;
mod transaction;
mod node;
pub mod config;
mod wip;

pub fn create_batch(batch_size: usize) {
    // const NB_ITER: u32 = 100;
    let mut batch = Vec::with_capacity(batch_size);
    for _ in 0..batch_size {
        let tx = wip::Transaction{
            from: 0,
            to: 1,
            amount: 42,
        };
        batch.push(tx);
    }
}

struct ExecutionResult {
    tx: Transaction,
    execution_start: Instant,
    execution_end: Instant,
    // state_change: StateChange,
}

#[async_trait]
trait VM {
    async fn execute(&mut self, mut backlog: Vec<Transaction>) -> Result<(Vec<ExecutionResult>, Duration)> {

        let mut to_process = backlog.len();
        let mut results = Vec::with_capacity(to_process);
        let start = Instant::now();

        loop {
            if to_process == 0 {
                let duration = start.elapsed();
                return Ok((results, duration));
            }

            self.dispatch(start, &mut backlog).await?;

            let (mut processed, mut conflicts) = self.collect().await?;

            to_process -= processed.len();
            results.append(&mut processed);
            backlog.append(&mut conflicts);
        }
    }

    async fn dispatch(&mut self, start: Instant, backlog: &mut Vec<Transaction>) -> Result<()>;

    async fn collect(&mut self) -> Result<(Vec<ExecutionResult>, Vec<Transaction>)>;
}

struct BasicVM {
    i: usize,
    tx_jobs: Vec<Sender<(Instant, Vec<Transaction>)>>,
    rx_results: Receiver<(Vec<ExecutionResult>, Vec<Transaction>)>
}

impl BasicVM {
    fn new(
        tx_jobs: Vec<Sender<(Instant, Vec<Transaction>)>>,
        rx_results: Receiver<(Vec<ExecutionResult>, Vec<Transaction>)>
    ) -> BasicVM {
        return BasicVM{ i: 0, tx_jobs, rx_results };
    }
}

#[async_trait]
impl VM for BasicVM {
    async fn dispatch(&mut self, start: Instant, backlog: &mut Vec<Transaction>) -> Result<()> {

        if !backlog.is_empty() {
            let nb_workers = self.tx_jobs.len();

            let mut worker_batches: Vec<Vec<Transaction>> = (0..nb_workers).map(|_| vec!()).collect();

            for tx in backlog.drain(0..backlog.len()) {
                worker_batches[tx.from as usize].push(tx);
            }

            for (i, batch) in worker_batches.drain(0..nb_workers).enumerate() {
                if !batch.is_empty() {
                    self.tx_jobs[i].send((start, batch)).await
                        .context("Unable to send job to worker")?;
                }
            }
        }

        return Ok(());
    }

    async fn collect(&mut self) -> Result<(Vec<ExecutionResult>, Vec<Transaction>)> {

        let collect_error: Error = anyhow::anyhow!("Unable to receive results from workers");

        // Another option would be to wait for all the workers to give their results
        return self.rx_results.recv().await
            .ok_or(collect_error);
    }
}

struct VmWorker {
    rx_jobs: Receiver<(Instant, Vec<Transaction>)>,
    tx_results: Sender<(Vec<ExecutionResult>, Vec<Transaction>)>
}

impl VmWorker {
    fn spawn(
        rx_jobs: Receiver<(Instant, Vec<Transaction>)>,
        tx_results: Sender<(Vec<ExecutionResult>, Vec<Transaction>)>
    ) {
        tokio::spawn(async move {

            let mut worker = VmWorker{ rx_jobs, tx_results };

            loop {
                match worker.rx_jobs.recv().await {
                    Some((start, batch)) => {
                        let mut results = Vec::with_capacity(batch.len());
                        let mut conflicts = vec!();
                        // Simulate actual work needing to be done?
                        // tokio::time::sleep(Duration::from_millis(1)).await;

                        let conflict = |tx: &Transaction| false; // dev
                        // TODO use filter and map instead
                        for tx in batch {
                            if conflict(&tx) {
                                conflicts.push(tx);
                            } else {
                                let result = ExecutionResult{
                                    tx,
                                    execution_start: start,
                                    execution_end: Instant::now(),
                                };

                                results.push(result);
                            }
                        }

                        let err = worker.tx_results.send((results, conflicts)).await;
                        if err.is_err() {
                            eprintln!("Unable to send execution results");
                            return;
                        }
                    },
                    None => {
                        return;
                    }
                }
            }
        });
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
    let (tx_result, rx_result) = channel(CHANNEL_CAPACITY);
    let mut tx_jobs = Vec::with_capacity(nb_nodes);

    for _ in 0..nb_nodes {
        let (tx_job, rx_job) = channel(CHANNEL_CAPACITY);
        VmWorker::spawn(rx_job, tx_result.clone());
        tx_jobs.push(tx_job);
    }

    let mut vm = BasicVM::new(tx_jobs, rx_result);

    // Benchmark -----------------------------------------------------------------------------------
    let mut batch = Vec::with_capacity(config.batch_size);
    let mut address: u64 = 0;
    for i in 0..config.batch_size {
        let tx = Transaction{
            from: address % nb_nodes as u64,
            to: nb_nodes as u64,
            amount: i as u64,
        };

        batch.push(tx);
        address += 1;
    }
    let (results, duration) = vm.execute(batch).await
        .context("Failed to execute batch")?;

    // Computing latency ---------------------------------------------------------------------------
    let mut processed: u64 = 0;
    let mut sum_latency = Duration::from_millis(0);
    let mut min_latency = Duration::MAX;
    let mut max_latency = Duration::from_nanos(0);

    for result in results {
        let latency = result.execution_end - result.execution_start;
        sum_latency += latency;
        min_latency = min_latency.min(latency);
        max_latency = max_latency.max(latency);
        processed += 1;
    }

    println!("Batch size: {}, tx processed: {}", config.batch_size, processed);
    println!("Executed {} tx in {:?}", config.batch_size, duration);
    let micro_throughput = config.batch_size as f64 / duration.as_micros() as f64;
    println!("Throughput is {} tx/µs", micro_throughput);
    println!("Throughput is {} tx/ms", micro_throughput * 1_000.0);
    println!("Throughput is {} tx/s", micro_throughput * 1_000_1000.0);
    println!();
    println!("Min latency is {:?}", min_latency);
    println!("Max latency is {:?}", max_latency);
    println!("Average latency is {:?} µs", sum_latency.as_micros() / processed as u128);
    println!("Average latency is {:?} ms", sum_latency.as_millis() / processed as u128);

    Ok(())
}