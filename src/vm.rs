use std::collections::{HashMap, VecDeque};
use anyhow::{Context, Result};
use async_trait::async_trait;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::time::Instant;

use crate::transaction::{Instruction, Transaction, TransactionAddress, TransactionOutput};
use crate::vm_implementation::SharedMemory;
use crate::wip::Word;
use crate::worker_implementation::WorkerInput;

pub const CHANNEL_CAPACITY: usize = 200;

pub type Batch = Vec<Transaction>;
pub type Jobs = Vec<Transaction>;
pub type BatchResult = Vec<TransactionOutput>;

// #[derive(Debug)]
// pub struct ExecutionResult {
//     pub output: TransactionOutput,
//     pub execution_end: Instant,
// }

#[derive(Debug)]
pub enum ExecutionResult {
    Output,
    Transaction(Transaction)
}

impl ExecutionResult {
    pub fn todo() -> Self {
        return ExecutionResult::Output;
    }
    pub fn is_done(&self) -> bool {
        match self {
            ExecutionResult::Output => true,
            _ => false,
        }
    }
}

#[async_trait]
pub trait VM {
    fn new(nb_workers: usize, batch_size: usize) -> Self where Self: Sized;

    async fn prepare(&mut self);

    async fn execute(&mut self, mut backlog: Jobs) -> Result<Vec<ExecutionResult>> {

        let mut to_process = backlog.len();
        let mut results = Vec::with_capacity(to_process);

        loop {
            if to_process == 0 {
                return Ok(results);
            }

            if !backlog.is_empty() {
                let mut conflicts = self.dispatch(&mut backlog).await?;
                backlog.append(&mut conflicts);
                // println!("Done dispatching");
            }

            let (mut processed, mut jobs) = self.collect().await?;
            // println!("Collected results: processed {}, conflict {}, backlog length {}", processed.len(), conflicts.len(), backlog.len());

            to_process -= processed.len();
            results.append(&mut processed);
            backlog.append(&mut jobs);
        }
    }

    async fn dispatch(&mut self, backlog: &mut Jobs) -> Result<Jobs>;

    async fn collect(&mut self) -> Result<(Vec<ExecutionResult>, Jobs)>;
}

#[async_trait]
pub trait VmWorker {

    fn new(
        rx_jobs: Receiver<Jobs>,
        tx_results: Sender<(Vec<ExecutionResult>, Jobs)>
    ) -> Self;

    async fn get_jobs(&mut self) -> Option<Jobs>;
    async fn send_results(&mut self, results: Vec<ExecutionResult>, conflicts: Jobs) -> Result<()>;

    async fn run(&mut self) -> Result<()> {
        loop {
            match self.get_jobs().await {
                Some(batch) => {
                    let mut results = Vec::with_capacity(batch.len());
                    let mut conflicts = vec!();

                    for tx in batch {
                        match self.execute(tx) {
                            Ok(output) => {
                                let result = ExecutionResult::Output;
                                results.push(result);
                            },
                            Err(conflict) => {
                                conflicts.push(conflict);
                            }
                        }
                    }

                    if let Err(e) = self.send_results(results, conflicts).await {
                        return Err(e).context("Unable to send execution results");
                    }

                },
                None => { return Ok(()); }
            }
        }
    }

    fn execute(&mut self, tx: Transaction) -> Result<TransactionOutput, Transaction>;
}

trait SpawnWorker{
    fn spawn(
        rx_jobs: Receiver<Jobs>,
        tx_results: Sender<(Vec<ExecutionResult>, Jobs)>
    );
}

impl<W> SpawnWorker for W where W: VmWorker + Send {
    fn spawn(
        rx_jobs: Receiver<Jobs>,
        tx_results: Sender<(Vec<ExecutionResult>, Jobs)>
    ) {
        tokio::spawn(async move {
            println!("Spawning basic worker");
            return Self::new(rx_jobs, tx_results)
                .run()
                .await;
        });
    }
}

pub trait WorkerPool{
    fn new_worker_pool(nb_workers: usize)
        -> (Vec<Sender<Jobs>>, Receiver<(Vec<ExecutionResult>, Jobs)>);
}

impl<W> WorkerPool for W where W: VmWorker + Send {
    fn new_worker_pool(nb_workers: usize)
        -> (Vec<Sender<Jobs>>, Receiver<(Vec<ExecutionResult>, Jobs)>)
    {
        let (tx_result, rx_results) = channel(nb_workers);
        let mut tx_jobs = Vec::with_capacity(nb_workers);

        for _ in 0..nb_workers {
            let (tx_job, rx_job) = channel(CHANNEL_CAPACITY);
            W::spawn(rx_job, tx_result.clone());
            tx_jobs.push(tx_job);
        }

        return (tx_jobs, rx_results);
    }
}

pub struct CPU;
impl CPU{
    pub fn execute_from_hashmap(
        instruction: &Instruction,
        stack: &mut VecDeque<u64>,
        memory: &mut HashMap<TransactionAddress, u64>,
    ) {
        match instruction {
            Instruction::CreateAccount(addr, balance) => {
                memory.insert(*addr, *balance);
            },
            Instruction::Increment(addr, amount) => {
                if memory.get_mut(&addr).is_none() {
                    println!("Addr = {}", addr);
                }
                *memory.get_mut(&addr).unwrap() += amount;
            },
            Instruction::Decrement(addr, amount) => {
                *memory.get_mut(&addr).unwrap() -= amount;
            },
            Instruction::Read(addr) => {
                let value = *memory.get(&addr).unwrap();
                stack.push_back(value);
            },
            Instruction::Write(addr) => {
                let value = stack.pop_back().unwrap();
                *memory.get_mut(&addr).unwrap() = value;
            },
            Instruction::Add() => {
                let a = stack.pop_back().unwrap();
                let b = stack.pop_back().unwrap();
                stack.push_front(a + b);
            },
            Instruction::Sub() => {
                let a = stack.pop_back().unwrap();
                let b = stack.pop_back().unwrap();
                stack.push_front(a - b);
            },
            Instruction::Push(amount) => {
                stack.push_back(*amount);
            },
            Instruction::Pop() => {
                stack.pop_back();
            }
        }
    }

    pub fn execute_from_array(
        instruction: &Instruction,
        stack: &mut VecDeque<u64>,
        memory: &mut Vec<Word>,
    ) {
        match instruction {
            Instruction::CreateAccount(addr, amount) => {
                if memory.get_mut(*addr as usize).is_none() {
                    println!("Address {}, memory size = ", addr);
                }
                *memory.get_mut(*addr as usize).unwrap() = *amount;
            },
            Instruction::Increment(addr, amount) => {
                *memory.get_mut(*addr as usize).unwrap() += amount;
            },
            Instruction::Decrement(addr, amount) => {
                *memory.get_mut(*addr as usize).unwrap() -= amount;
            },
            Instruction::Read(addr) => {
                let value = *memory.get(*addr as usize).unwrap();
                stack.push_back(value);
            },
            Instruction::Write(addr) => {
                let value = stack.pop_back().unwrap();
                *memory.get_mut(*addr as usize).unwrap() = value;
            },
            Instruction::Add() => {
                let a = stack.pop_back().unwrap();
                let b = stack.pop_back().unwrap();
                stack.push_front(a + b);
            },
            Instruction::Sub() => {
                let a = stack.pop_back().unwrap();
                let b = stack.pop_back().unwrap();
                stack.push_front(a - b);
            },
            Instruction::Push(amount) => {
                stack.push_back(*amount);
            },
            Instruction::Pop() => {
                stack.pop_back();
            }
        }
    }

    pub fn execute_from_shared(
        instruction: &Instruction,
        stack: &mut VecDeque<u64>,
        memory: &mut SharedMemory,
    ) {
        match instruction {
            Instruction::CreateAccount(addr, amount) => {
                memory.set(*addr as usize, *amount);
            },
            Instruction::Increment(addr, amount) => {
                let balance = memory.get(*addr as usize);
                memory.set(*addr as usize, balance + *amount);
            },
            Instruction::Decrement(addr, amount) => {
                let balance = memory.get(*addr as usize);
                memory.set(*addr as usize, balance - *amount);
            },
            Instruction::Read(addr) => {
                let value = memory.get(*addr as usize);
                stack.push_back(value);
            },
            Instruction::Write(addr) => {
                let value = stack.pop_back().unwrap();
                memory.set(*addr as usize, value);
            },
            Instruction::Add() => {
                let a = stack.pop_back().unwrap();
                let b = stack.pop_back().unwrap();
                stack.push_front(a + b);
            },
            Instruction::Sub() => {
                let a = stack.pop_back().unwrap();
                let b = stack.pop_back().unwrap();
                stack.push_front(a - b);
            },
            Instruction::Push(amount) => {
                stack.push_back(*amount);
            },
            Instruction::Pop() => {
                stack.pop_back();
            }
        }
    }
}
