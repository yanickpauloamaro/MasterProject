use std::collections::{HashMap, VecDeque};

use anyhow::Result;

use crate::transaction::{Instruction, Transaction, TransactionAddress, TransactionOutput};
use crate::vm_utils::{SharedStorage};
use crate::wip::Word;

// pub const CHANNEL_CAPACITY: usize = 200;

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
    fn execute(&mut self, mut _backlog: Jobs) -> Result<Vec<ExecutionResult>> {
        todo!();
    }

    fn set_storage(&mut self, value: Word);
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
        stack: &mut VecDeque<Word>,
        storage: &mut HashMap<TransactionAddress, Word>,
    )
    {
        match instruction {
            Instruction::CreateAccount(addr, balance) => {
                storage.insert(*addr, *balance);
            },
            Instruction::Increment(addr, amount) => {
                *storage.get_mut(&addr).unwrap() += amount;
            },
            Instruction::Decrement(addr, amount) => {
                *storage.get_mut(&addr).unwrap() -= amount;
            },
            Instruction::Read(addr) => {
                let value = *storage.get(&addr).unwrap();
                stack.push_back(value);
            },
            Instruction::Write(addr) => {
                let value = stack.pop_back().unwrap();
                *storage.get_mut(&addr).unwrap() = value;
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
        stack: &mut VecDeque<Word>,
        storage: &mut Vec<Word>,
    )
    {
        match instruction {
            Instruction::CreateAccount(addr, amount) => {
                if storage.get_mut(*addr as usize).is_none() {
                    println!("Address {}, storage size = ", addr);
                }
                *storage.get_mut(*addr as usize).unwrap() = *amount;
            },
            Instruction::Increment(addr, amount) => {
                *storage.get_mut(*addr as usize).unwrap() += amount;
            },
            Instruction::Decrement(addr, amount) => {
                *storage.get_mut(*addr as usize).unwrap() -= amount;
            },
            Instruction::Read(addr) => {
                let value = *storage.get(*addr as usize).unwrap();
                stack.push_back(value);
            },
            Instruction::Write(addr) => {
                let value = stack.pop_back().unwrap();
                *storage.get_mut(*addr as usize).unwrap() = value;
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
        stack: &mut VecDeque<Word>,
        storage: &mut SharedStorage,
    )
    {
        match instruction {
            Instruction::CreateAccount(addr, amount) => {
                storage.set(*addr as usize, *amount);
            },
            Instruction::Increment(addr, amount) => {
                let balance = storage.get(*addr as usize);
                storage.set(*addr as usize, balance + *amount);
            },
            Instruction::Decrement(addr, amount) => {
                let balance = storage.get(*addr as usize);
                storage.set(*addr as usize, balance - *amount);
            },
            Instruction::Read(addr) => {
                let value = storage.get(*addr as usize);
                stack.push_back(value);
            },
            Instruction::Write(addr) => {
                let value = stack.pop_back().unwrap();
                storage.set(*addr as usize, value);
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
