use std::collections::VecDeque;
use std::time::Instant;

use anyhow::Result;

use crate::transaction::TransactionOutput;
use crate::vm::{CPU, ExecutionResult, Executor, Jobs};
use crate::wip::Word;

//region Serial VM =================================================================================
pub struct VMa {
    memory: Vec<Word>
}

impl VMa {
    pub fn new(memory_size: usize) -> Result<Self> {
        let memory = (0..memory_size).map(|_| 0 as Word).collect();
        let vm = Self{ memory };
        return Ok(vm);
    }
}

impl Executor for VMa {
    // TODO Add iterations to VMa
    fn execute(&mut self, mut _backlog: Jobs) -> Result<Vec<ExecutionResult>> {
let start = Instant::now();
        let mut results = Vec::with_capacity(_backlog.len());
        let mut stack = VecDeque::new();
        for tx in _backlog {
            for instr in tx.instructions.iter() {
                CPU::execute_from_array(instr, &mut stack, &mut self.memory);
            }
            let _output = TransactionOutput{ tx };
            let result = ExecutionResult::todo();
            results.push(result);
            stack.clear();
        }
println!("### Done serial execution in {:?}", start.elapsed());
        return Ok(results);
    }

    fn set_memory(&mut self, value: Word) {
        self.memory.fill(value);
    }
}
//endregion