#![allow(unused_variables)]
use crate::config::Config;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::{broadcast, oneshot};
use tokio::time::Instant;
use either::Either;
use std::time::Duration;
use std::mem;
use anyhow::{self, Context, Result};
// use leaky_bucket;
use hwloc::Topology;
use crate::basic_vm::BasicVM;

use crate::transaction::Transaction;
use crate::utils::{compatible, get_nb_nodes, print_metrics};
use crate::vm::{CHANNEL_CAPACITY, VM};

type WorkerResult = Either<Vec<Transaction>, ()>;
pub type Log = (u64, Instant, Vec<Instant>); // block id, block creation time

// const BLOCK_SIZE: usize = 64 * 1024;
// const BLOCK_SIZE: usize = 100;
const DEV_RATE: u32 = 100;

pub async fn parse_logs(config: &Config, rx_logs: &mut Vec<oneshot::Receiver<Vec<Log>>>) {
    let mut processed: u64 = 0;
    let mut sum_latency = Duration::from_millis(0);
    let mut min_latency = Duration::MAX;
    let mut max_latency = Duration::from_nanos(0);

    println!();
    println!("Collecting logs from workers:");
    for (i, rx) in rx_logs.iter_mut().enumerate() {
        match rx.await {
            Ok(block_logs) => {
                println!("Worker {} processed {} blocks", i, block_logs.len());
                for (_block_id, block_creation, logs) in block_logs {
                    // println!("Block {} contains {} tx", block_id, logs.len());
                    processed += logs.len() as u64;
                    for tx_completion in logs {
                        let latency = tx_completion - block_creation;
                        // println!("\tA tx took {:?}", latency);
                        sum_latency += latency;
                        min_latency = min_latency.min(latency);
                        max_latency = max_latency.max(latency);
                    }
                }
            }
            Err(e) => println!("Failed to receive log from worker {}: {:?}", i, e)
        }
    }

    println!();
    println!("Processed {} tx in {} s", processed, config.duration);
    println!("Throughput is {} tx/s", processed/config.duration);
    println!("Min latency is {:?}", min_latency);
    println!("Max latency is {:?}", max_latency);
    println!("Average latency is {:?} µs", sum_latency.as_micros() / processed as u128);
    println!("Average latency is {:?} ms", sum_latency.as_millis() / processed as u128);
    println!();
}

pub struct TransactionGenerator {
    /* TODO Check that the generator is fast enough (Throughput > 5 millions/s)
         Otherwise, need to use multiple generators
    */
    pub tx: Sender<Transaction>,
}

impl TransactionGenerator {
    async fn trigger(interval: &mut tokio::time::Interval) {
        async {}.await;
        // interval.tick().await;
    }

    pub fn spawn(self, mut signal: broadcast::Receiver<()>) {
        tokio::spawn(async move {
            println!("<Transaction generator started");

            let duration = Duration::from_secs(1)
                .checked_div(DEV_RATE)
                .unwrap_or(Duration::from_millis(100)); // 10 tx /s
            let mut interval = tokio::time::interval(duration);

            loop {
                tokio::select! {
                    biased;
                    _ = signal.recv() => {
                        println!(">Transaction generator stopped");
                        return;
                    },
                    _ = TransactionGenerator::trigger(&mut interval) => {
                        let tx = Transaction{
                            from: 0,
                            to: 1,
                            amount: 42,
                        };
                        // TODO Generate blocks of transactions to reduce overhead
                        // let block: Vec<Transaction> = (0..batch_size).map(|_| tx).collect();

                        if let Err(e) = self.tx.send(tx).await {
                            eprintln!("Failed to send tx to rate limiter");
                        }
                    }
                }
            }
        });
    }
}

pub struct RateLimiter {
    pub rx: Receiver<Transaction>,
    pub tx: Sender<(Instant, Vec<Transaction>)>,
}

impl RateLimiter {
    pub fn spawn(mut self, mut signal: broadcast::Receiver<()>, batch_size: usize, rate: u32) {
        tokio::spawn(async move {
            println!("<Rate limiter started");

            let mut acc: Vec<Transaction> = Vec::with_capacity(batch_size);
            let duration = Duration::from_secs(1).checked_div(rate)
                .expect("Unable to compute rate limiter interval");

            let loop_start = Instant::now();

            loop {
                tokio::select! {
                    biased;
                    _ = signal.recv() => {
                        println!(">Rate limiter stopped");
                        return;
                    },
                    Some(tx) = self.rx.recv() => {
                        acc.push(tx);

                        if acc.len() >= batch_size {
                            let mut block = Vec::with_capacity(batch_size);
                            mem::swap(&mut block, &mut acc);

                            let creation = Instant::now();
                            if let Err(e) = self.tx.send((creation, block)).await {
                               eprintln!("Failed to send tx to client");
                            }
                        }
                    },
                }
            }
        });
    }
}

pub struct Client {
    pub rx_block: Receiver<(Instant, Vec<Transaction>)>,
    pub tx_jobs: Vec<Sender<(Instant, Vec<Transaction>)>>,
    pub rx_results: Vec<Receiver<WorkerResult>>
}

impl Client {
    pub fn spawn(mut self, mut signal: broadcast::Receiver<()>) {
        tokio::spawn(async move {
            println!("<Client started");
            let mut i = 0;
            let nb_workers = self.tx_jobs.len();

            loop {
                tokio::select! {
                _ = signal.recv() => {
                    println!(">Client stopped");
                    return;
                },

                Some((creation, block)) = self.rx_block.recv() => {
                    // TODO Dispatch properly
                    if let Err(e) = self.tx_jobs[i % nb_workers].send((creation, block)).await {
                        eprintln!("Failed to send jobs to worker");
                    }

                    i += 1;
                }
            }
            }
        });
    }
}

pub struct Worker {
    pub rx_job: Receiver<(Instant, Vec<Transaction>)>,
    pub backlog: Vec<Transaction>,
    pub logs: Vec<Log>,
    pub tx_result: Sender<WorkerResult>,
    pub tx_log: oneshot::Sender<Vec<Log>>,
}

impl Worker {
    pub fn spawn(mut self, mut signal: broadcast::Receiver<()>) {
        tokio::spawn(async move {
            println!("<Worker started");
            let mut i = 0;
            loop {
                tokio::select! {
                _ = signal.recv() => {

                    println!(">Worker stopped");
                    let nb_logs = self.logs.len();
                    if let Err(e) = self.tx_log.send(mem::take(&mut self.logs)) {
                        eprintln!("Failed to send logs to benchmark");
                    }
                    return;
                },

                Some((creation, block)) = self.rx_job.recv() => {
                    // TODO Take conflicts into consideration
                    // let backlog = vec!();
                    let mut log = vec!();
                    for tx in block {
                        // println!("Working on {:?}", tx);
                        // tokio::time::sleep(Duration::from_millis(100)).await;
                        let completion = Instant::now();
                        log.push(completion);
                    }
                    self.logs.push((i, creation, log));
                    i += 1;
                }
            }
            }
        });
    }
}

// Allocate "VM address space" (split per NUMA region?)
// Create nodes (represent NUMA regions?)
// Set memory policy so that memory is allocated locally to each core
// TODO Check how to send transaction to each node
// Create Backlog
// Create dispatcher (will send the transactions to the different regions)
// Create client (will generate transactions)
pub async fn benchmark_rate(config: Config) -> Result<()> {

    // Setup ---------------------------------------------------------------------------------------
    let topo = Topology::new();

    // Check compatibility with core pinning
    compatible(&topo)?;

    // Determine number of cores to use
    let nb_nodes = get_nb_nodes(&topo, &config)?;
    // let share = config.address_space_size / nb_nodes;

    // TODO Move initialisation into spawn and use a broadcast variable to trigger the start of the benchmark
    let (tx_generator, rx_rate) = channel(CHANNEL_CAPACITY);
    let (tx_rate, mut rx_client) = channel(CHANNEL_CAPACITY);
    let generator = TransactionGenerator{
        tx: tx_generator
    };
    let rate_limiter = RateLimiter{
        rx: rx_rate,
        tx: tx_rate
    };

    let mut vm = BasicVM::new(nb_nodes);
    // vm.prepare().await;

    let (tx_stop, _) = broadcast::channel(1);
    rate_limiter.spawn(tx_stop.subscribe(), config.batch_size, config.rate);
    generator.spawn(tx_stop.subscribe());

    return tokio::spawn(async move {
        println!();
        println!("Benchmarking rate {}s:", config.duration);
        let benchmark_start = Instant::now();
        let timer = tokio::time::sleep(Duration::from_secs(config.duration));
        let mut batch_results = Vec::with_capacity(config.batch_size);
        tokio::pin!(timer);

        loop {
            tokio::select! {
                biased;
                () = &mut timer => {
                    tx_stop.send(()).context("Unable to send stop signal")?;
                    break;
                },
                Some((creation, batch)) = rx_client.recv() => {
                    if benchmark_start.elapsed() > Duration::from_secs(config.duration) {
                        tx_stop.send(()).context("Unable to send stop signal")?;
                        break;
                    }

                    let start = Instant::now();
                    let result = vm.execute(batch).await?;
                    let duration = start.elapsed();

                    batch_results.push((result, start, duration));
                }
            }
        }

        let total_duration = benchmark_start.elapsed();
        println!("Done benchmarking");
        println!();
        // println!("Processed {} batches of {} tx in {:?} s",
        //          batch_processed, config.batch_size, total_duration);
        // println!("Throughput is {} tx/s",
        //          (batch_processed * config.batch_size as u64)/ total_duration.as_secs());
        // println!();

        print_metrics(batch_results, total_duration);
        println!();

        Ok(())
    }).await?;
}

