use std::collections::{HashMap, VecDeque};
use std::time::Duration;
use tokio::time::Instant;
use async_trait::async_trait;
use crate::basic_vm::BasicWorker;
use crate::transaction::{Instruction, Transaction, TransactionAddress, TransactionOutput};
use crate::vm::{ExecutionResult, Jobs, VM, CPU};


pub struct SerialVM {
    pub data: HashMap<TransactionAddress , u64>
}

#[async_trait]
impl VM for SerialVM {
    fn new(nb_workers: usize, batch_size: usize) -> Self {
        Self{
            data: HashMap::new()
        }
    }

    async fn prepare(&mut self) {
        tokio::time::sleep(Duration::from_secs(2)).await;
    }

    async fn execute(&mut self, backlog: Vec<Transaction>) -> anyhow::Result<Vec<ExecutionResult>> {
        let mut results = vec!();
        for tx in backlog {
            let mut stack = VecDeque::new();
            for instr in tx.instructions.iter() {
                CPU::execute_from_hashmap(instr, &mut stack, &mut self.data);
            }
            let output = TransactionOutput{ tx };
            let result = ExecutionResult::Output;
            results.push(result);
        }

        return Ok(results);
    }

    async fn dispatch(&mut self, backlog: &mut Jobs) -> anyhow::Result<Jobs> {
        todo!()
    }

    async fn collect(&mut self) -> anyhow::Result<(Vec<ExecutionResult>, Jobs)> {
        todo!()
    }
}