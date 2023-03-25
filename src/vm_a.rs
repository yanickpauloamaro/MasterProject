use std::collections::VecDeque;
use std::mem;
use std::time::Instant;

use anyhow::Result;

use crate::{debug, debugging};
use crate::vm::{CPU, ExecutionResult, Executor, Jobs};
use crate::wip::Word;

//region Serial VM =================================================================================
pub struct VMa {
    storage: Vec<Word>
}

impl VMa {
    pub fn new(storage_size: usize) -> Result<Self> {
        let storage = (0..storage_size).map(|_| 0 as Word).collect();
        let vm = Self{ storage };
        return Ok(vm);
    }
}

impl Executor for VMa {
    // TODO Add iterations to VMa
    fn execute(&mut self, mut batch: Jobs) -> Result<Vec<ExecutionResult>> {
let start = Instant::now();
        let mut results = Vec::with_capacity(batch.len());
        let mut backlog = Vec::with_capacity(batch.len());

        let mut stack = VecDeque::new();

        loop {
            if batch.is_empty() {
debug!("### Done serial execution in {:?}\n", start.elapsed());
                return Ok(results);
            }

            for tx in batch.iter() {
                stack.clear(); // TODO Does this need to be optimised?
                for instr in tx.instructions.iter() {
                    CPU::execute_from_array(instr, &mut stack, &mut self.storage);
                }
                let result = ExecutionResult::todo();
                results.push(result);
                // TODO add to next transaction piece to backlog
            }

            batch.drain(0..).for_each(std::mem::drop);
            mem::swap(&mut batch, &mut backlog);
        }
    }

    fn set_storage(&mut self, value: Word) {
        self.storage.fill(value);
    }
}
//endregion