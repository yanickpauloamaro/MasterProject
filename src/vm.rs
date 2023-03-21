use std::collections::{HashMap, VecDeque};
use anyhow::{Context, Result};
use async_trait::async_trait;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::time::Instant;

use crate::transaction::{Instruction, Transaction, TransactionAddress, TransactionOutput};
use crate::vm_implementation::SharedMemory;
use crate::wip::Word;
use crate::worker_implementation::WorkerInput;

// pub const CHANNEL_CAPACITY: usize = 200;

pub type Batch = Vec<Transaction>;
pub type Jobs = Vec<Transaction>;
pub type BatchResult = Vec<TransactionOutput>;

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

pub trait Executor {
    fn execute(&mut self, mut backlog: Jobs) -> Result<Vec<ExecutionResult>> {
        todo!();
    }

    fn set_memory(&mut self, value: Word);
}

// trait SpawnWorker{
//     fn spawn(
//         rx_jobs: Receiver<Jobs>,
//         tx_results: Sender<(Vec<ExecutionResult>, Jobs)>
//     );
// }
//
// impl<W> SpawnWorker for W where W: VmWorker + Send {
//     fn spawn(
//         rx_jobs: Receiver<Jobs>,
//         tx_results: Sender<(Vec<ExecutionResult>, Jobs)>
//     ) {
//         tokio::spawn(async move {
//             println!("Spawning basic worker");
//             return Self::new(rx_jobs, tx_results)
//                 .run()
//                 .await;
//         });
//     }
// }

// pub trait WorkerPool{
//     fn new_worker_pool(nb_workers: usize)
//         -> (Vec<Sender<Jobs>>, Receiver<(Vec<ExecutionResult>, Jobs)>);
// }
//
// impl<W> WorkerPool for W where W: VmWorker + Send {
//     fn new_worker_pool(nb_workers: usize)
//         -> (Vec<Sender<Jobs>>, Receiver<(Vec<ExecutionResult>, Jobs)>)
//     {
//         let (tx_result, rx_results) = channel(nb_workers);
//         let mut tx_jobs = Vec::with_capacity(nb_workers);
//
//         for _ in 0..nb_workers {
//             let (tx_job, rx_job) = channel(CHANNEL_CAPACITY);
//             W::spawn(rx_job, tx_result.clone());
//             tx_jobs.push(tx_job);
//         }
//
//         return (tx_jobs, rx_results);
//     }
// }

pub struct CPU;
impl CPU{
    pub fn execute_from_hashmap(
        instruction: &Instruction,
        stack: &mut VecDeque<u64>,
        memory: &mut HashMap<TransactionAddress, u64>,
    )
    {
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
    )
    {
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
    )
    {
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
