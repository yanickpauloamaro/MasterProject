use async_trait::async_trait;
use std::collections::VecDeque;
use crate::transaction::TransactionOutput;
use crate::vm::{CPU, ExecutionResult, Jobs};
use crate::wip::{Executor, Word};

// Serial VM =======================================================================================
pub struct VMa {
    memory: Vec<Word>
}

impl VMa {
    pub fn new(memory_size: usize) -> anyhow::Result<Self> {
        let memory = (0..memory_size).map(|_| 0 as Word).collect();
        let vm = Self{ memory };
        return Ok(vm);
    }
}

#[async_trait]
impl Executor for VMa {
    async fn execute(&mut self, mut backlog: Jobs) -> anyhow::Result<Vec<ExecutionResult>> {
        let mut results = Vec::with_capacity(backlog.len());
        let mut stack = VecDeque::new();
        for tx in backlog {
            for instr in tx.instructions.iter() {
                CPU::execute_array(instr, &mut stack, &mut self.memory);
            }
            let output = TransactionOutput{ tx };
            let result = ExecutionResult::todo();
            results.push(result);
            stack.clear();
        }

        return Ok(results);
    }
}
