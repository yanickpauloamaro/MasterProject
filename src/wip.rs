#![allow(unused_variables)]
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::{broadcast, oneshot};
use tokio::time::{Instant, Duration};
use either::Either;
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

let start = Instant::now();
let mut finding_assigned_workers = Duration::from_micros(0);
let mut rest = Duration::from_micros(0);
        let mut worker_jobs: Vec<Jobs> = (0..self.nb_workers).map(|_| vec!()).collect();
        let mut conflicting_jobs: Jobs = vec!();
        let mut next_worker = 0;
        for tx in backlog.drain(..backlog.len()) {
            // Find which worker is responsible for the addresses in this transaction
let mut a = Instant::now();
            let mut assigned_workers = vec!();
            for w in 0..self.nb_workers {
                if self.filters[w].has(&tx) {
                    assigned_workers.push(w);
                }
            }
finding_assigned_workers += a.elapsed();
a = Instant::now();
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
rest += a.elapsed();
        }
println!("Separating transactions took {:?}", start.elapsed());
println!("\tIncluding {:?} for finding assigned workers", finding_assigned_workers);
println!("\tIncluding {:?} for the rest", rest);
println!();

        // Send jobs to each worker
        debug!("Sending jobs to workers...");
let start = Instant::now();
        for (worker, jobs) in worker_jobs.into_iter().enumerate() {
            // N.B: Must send to worker even if the jobs contains no transaction because ::collect
            // expects all workers to send a result
            self.tx_jobs[worker].send(jobs).await
                .context(format!("Unable to send job to worker"))?;
        }
println!("Sending transactions took {:?}", start.elapsed());
println!();

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