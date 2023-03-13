#![allow(unused_variables)]

use std::collections::{HashMap, LinkedList, VecDeque};
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::{broadcast, oneshot, RwLock};
use tokio::time::{Duration, Instant};
use either::Either;
use std::{mem, thread};
use std::sync::Arc;
use anyhow::{anyhow, Context, Error, Result};
use async_trait::async_trait;
use bloomfilter::Bloom;
use hwloc::Topology;
use tokio::task::JoinHandle;
use async_recursion::async_recursion;

use crate::basic_vm::{BasicVM, BasicWorker};
use crate::transaction::{Instruction, Transaction, TransactionAddress, TransactionOutput};
use crate::config::Config;
use crate::{debug, debugging};
use crate::utils::{compatible, get_nb_nodes, print_metrics};
use crate::vm::{Batch, CHANNEL_CAPACITY, CPU, ExecutionResult, Jobs, VM, WorkerPool};

#[async_trait]
pub trait Executor {
    async fn execute(&mut self, mut backlog: Jobs) -> Result<Vec<ExecutionResult>> {
        todo!();
    }

    fn execute_instruction(&mut self) {
        todo!();
    }
}
#[derive(Debug)]
pub struct WorkerInput {
    // jobs: &Jobs
    assignments: Arc<Vec<usize>>,
    batch: Arc<Jobs>,
    memory: Arc<Vec<Word>>,
    // test: *mut Word,
}
pub type WorkerOutput = (Vec<ExecutionResult>, Vec<Transaction>);

pub struct WorkerB {
    index: usize,
    tx_job: Sender<WorkerInput>,
    rx_result: Receiver<WorkerOutput>
}

impl WorkerB {
    fn new(
        index: usize,
    ) -> Self {

        let (tx_job, mut rx_job) = channel(1);
        let (tx_result, rx_result) = channel(1);

        let index = index;
        tokio::spawn(async move {
            println!("Spawned worker {}", index);

            loop {
                match rx_job.recv().await {
                    Some(job) => {
                        let mut job: WorkerInput = job;
                        // println!("Worker {} is handling {:?}", index, job);
                        let mut worker_output = vec!();
                        let mut worker_backlog = vec!();
                        // TODO use memory from vm
                        let mut memory: Vec<Word> = vec![100; 256];
                        // let mut memory = job.memory.as_mut_ptr();
                        // let mut memory = job.memory;

                        for tx in job.batch.iter() {

                            let assigned_to_self = job.assignments[tx.from as usize] == index &&
                                job.assignments[tx.to as usize] == index;

                            if !assigned_to_self {
                                continue;
                            }

                            let mut stack: VecDeque<Word> = VecDeque::new();
                            for instr in tx.instructions.iter() {
                                CPU::execute_array(instr, &mut stack, &mut memory);
                            }

                            let result = ExecutionResult::todo();
                            worker_output.push(result);
                        }
                        let result_msg = (worker_output, worker_backlog);
                        if let Err(e) = tx_result.send(result_msg).await {
                            println!("Worker {} can't send result: {}", index, e);
                        }
                    },
                    None => {
                        println!("Worker {} stops", index);
                        break;
                    }
                }
            }
        });
        return Self {
            index,
            tx_job,
            rx_result,
        };
    }

    async fn send(&mut self, jobs: WorkerInput) -> Result<()> {
        self.tx_job.send(jobs).await?;
        Ok(())
    }

    async fn receive(&mut self) -> Result<WorkerOutput> {
        self.rx_result.recv().await.ok_or(anyhow!("Worker {}: unable to receive results", self.index))
    }
}

pub type Word = u64;
pub type Address = u64;

// Parallel VM =====================================================================================
// memory: Hashmap
// dispatch: bloom filter to postpone conflicts
// communication: message passing
pub struct VMb {
    // memory: Arc<RwLock<HashMap<TransactionAddress , Word>>>,
    memory: Arc<Vec<Word>>,
    // stack: VecDeque<Word>,

    // bloom_set: BloomFilter,
    nb_workers: usize,
    // assignment: Vec<usize>,

    workers: Vec<WorkerB>,
    // tx_jobs: Vec<Sender<(Arc<Vec<usize>>, Arc<Vec<Transaction>>)>>,
    // rx_results: Receiver<(Vec<ExecutionResult>, Vec<Transaction>)>,
    // results: Vec<ExecutionResult>,
}
impl VMb {
    pub fn new(memory_size: usize, nb_workers: usize, batch_size: usize) -> Result<Self> {
        // const FALSE_POSITIVE_RATE: f64 = 0.1;
        let memory= Arc::new(vec![0 as u64; memory_size]);

        let mut workers = Vec::with_capacity(nb_workers);
        for index in 0..nb_workers {
            workers.push(WorkerB::new(index));
        }

        let vm = Self{ memory, nb_workers, workers };
        return Ok(vm);
    }

    #[async_recursion]
    async fn execute_rec(&mut self,
                         mut results: Vec<ExecutionResult>,
                         mut batch: Jobs,
                         mut backlog: Jobs,
                         mut assigned_workers: Vec<usize>
    ) -> Result<Vec<ExecutionResult>>
    {
        if batch.is_empty() {
            return Ok(results);
        }
        // println!("\n*** execute rec, assigned_worker length {}", assigned_workers.len());
        const CONFLICT: usize = usize::MAX;
        const DONE: usize = CONFLICT - 1;
        const NONE: usize = CONFLICT - 2;

        // Assign jobs to workers ------------------------------------------------------------------
        let mut next_worker = 0;
        assigned_workers.fill(NONE);

        for tx in batch.iter() {
            let from = tx.from as usize;
            let to = tx.to as usize;

            let worker_from = assigned_workers[from];
            let worker_to = assigned_workers[to];

            let assigned = match (worker_from, worker_to) {
                (NONE, NONE) => {
                    // println!("Neither address is assigned: from={}, to={}", from, to);
                    let worker = next_worker;
                    next_worker = (next_worker + 1) % self.nb_workers;
                    assigned_workers[from] = worker;
                    assigned_workers[to] = worker;
                    worker
                },
                (worker, NONE) => {
                    // println!("First address is assigned to {}: from={}, to={}", worker, from, to);
                    assigned_workers[to] = worker;
                    worker
                },
                (NONE, worker) => {
                    // println!("Second address is assigned to {}: from={}, to={}", worker, from, to);
                    assigned_workers[from] = worker;
                    worker
                },
                (a, b) if a == b => {
                    // println!("Both addresses are assigned to {}: from={}, to={}", a, from, to);
                    a
                },
                (a, b) => {
                    // println!("Both addresses are assigned to different workers: from={}->{}, to={}->{}", from, a, to, b);
                    CONFLICT
                },
            };

            if assigned == CONFLICT {
                backlog.push(tx.clone());
            }
        }

        // Start parallel execution ----------------------------------------------------------------
        let read_only_batch = Arc::new(batch);
        let read_only_assignments = Arc::new(assigned_workers);

        for worker in self.workers.iter_mut() {
            /* TODO How to give access to "assigned_workers", "batch" and "self.memory" without
                copying or move the values?
            */
            // TODO Send each worker a list of indexes of transactions
            let input = WorkerInput{
                assignments: read_only_assignments.clone(),
                batch: read_only_batch.clone(),
                memory: self.memory.clone(),
            };
            if let Err(e) = worker.send(input).await {
                println!("VM: Failed to send work to worker {}", worker.index);
            }
        }

        // Collect results -------------------------------------------------------------------------
        for worker in self.workers.iter_mut() {
            let (mut worker_output, mut worker_backlog) = worker.receive().await?;
            results.append(&mut worker_output);
            backlog.append(&mut worker_backlog);
        }

        let assigned_workers = Arc::try_unwrap(read_only_assignments).unwrap();
        let mut next_backlog = Arc::try_unwrap(read_only_batch).unwrap();
        next_backlog.clear();

        return self.execute_rec(results, backlog, next_backlog, assigned_workers).await;
    }
}

#[async_trait]
impl Executor for VMb {
    async fn execute(&mut self, mut batch: Jobs) -> Result<Vec<ExecutionResult>> {

        let mut results = Vec::with_capacity(batch.len());
        let mut backlog = Vec::with_capacity(batch.len());
        let mut assigned_workers = vec![usize::MAX; self.memory.len()];

        return self.execute_rec(results, batch, backlog, assigned_workers).await;
    }
}

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

}
