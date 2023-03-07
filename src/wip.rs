#![allow(unused_variables)]
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::{broadcast, oneshot};
use tokio::time::Instant;
use either::Either;
use std::time::Duration;
use std::mem;
use anyhow::{self, Context, Error, Result};
use async_trait::async_trait;
use bloomfilter::Bloom;
use hwloc::Topology;

use crate::basic_vm::{BasicVM, BasicWorker};
use crate::transaction::{Transaction, TransactionAddress};
use crate::config::Config;
use crate::{debug, debugging};
use crate::utils::{compatible, get_nb_nodes, print_metrics};
use crate::vm::{Batch, CHANNEL_CAPACITY, ExecutionResult, Jobs, VM, WorkerPool};

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

        let mut worker_jobs: Vec<Jobs> = (0..self.nb_workers).map(|_| vec!()).collect();
        let mut conflicting_jobs: Jobs = vec!();
        let mut next_worker = 0;

        for tx in backlog.drain(..backlog.len()) {
            // Find which worker is responsible for the addresses in this transaction
            let mut assigned_workers = vec!();
            for w in 0..self.nb_workers {
                if self.filters[w].has(&tx) {
                    assigned_workers.push(w);
                }
            }

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

            // Assign the transaction to the worker
            self.filters[assignee].insert(&tx);
            worker_jobs[assignee].push(tx);
        }

        // Send jobs to each worker
        debug!("Sending jobs to workers...");
        for (worker, jobs) in worker_jobs.into_iter().enumerate() {
            // N.B: Must send to worker even if the jobs contains no transaction because ::collect
            // expects all workers to send a result
            self.tx_jobs[worker].send(jobs).await
                .context(format!("Unable to send job to worker"))?;
        }

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





















// =================================================================================================
type WorkerResult = Either<Vec<Transaction>, ()>;
pub type Log = (u64, Instant, Vec<Instant>); // block id, block creation time

// const BLOCK_SIZE: usize = 64 * 1024;
// const BLOCK_SIZE: usize = 100;
const DEV_RATE: u32 = 100;

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

    let mut vm = BasicVM::new(nb_nodes, config.batch_size);
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

