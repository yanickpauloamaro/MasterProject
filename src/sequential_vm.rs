use std::time::Duration;
use strum::IntoEnumIterator;
use tokio::time::Instant;
use crate::contract::{AtomicFunction, FunctionResult, Transaction};
use crate::contract::FunctionResult::Another;
use crate::vm::Executor;
use crate::vm_utils::{SharedStorage};
use crate::wip::Word;

#[derive(Debug)]
pub struct SequentialVM {
    pub storage: Vec<Word>,
    functions: Vec<AtomicFunction>,
}

impl SequentialVM {
    pub fn new(storage_size: usize) -> anyhow::Result<Self> {
        let storage = vec![0; storage_size];
        let functions = AtomicFunction::iter().collect();

        let vm = Self{ storage, functions };
        return Ok(vm);
    }

    pub fn set_storage(&mut self, value: Word) {
        self.storage.fill(value);
    }

    pub fn init_storage(&mut self, init: Box<dyn Fn(&mut Vec<Word>)>) {
        init(&mut self.storage)
    }

    pub fn execute<const A: usize, const P: usize>(&mut self, mut batch: Vec<Transaction<A, P>>) -> anyhow::Result<(Duration, Duration)> {

        let storage = SharedStorage{ ptr: self.storage.as_mut_ptr(), size: self.storage.len() };

        let execution_start = Instant::now();

        while !batch.is_empty() {
            let tx = batch.pop().unwrap();
            // let function = self.functions.get(tx.function as usize).unwrap();
            let function = tx.function;
            match unsafe { function.execute(tx, storage) } {
                Another(generated_tx) => {
                    batch.push(generated_tx);
                },
                _ => {
                    continue;
                }
            }
        }

        // 'outer: while let Some(mut next_tx) = batch.pop() {
        //     let mut tx = next_tx;
        //     while let Another(generated_tx) = unsafe { tx.function.execute(tx, storage) } {
        //         tx = generated_tx;
        //     }
        // }

        return Ok((Duration::from_micros(0), execution_start.elapsed()));
    }

    pub fn execute_with_results<const A: usize, const P: usize>(&mut self, mut batch: Vec<Transaction<A, P>>) -> anyhow::Result<Vec<FunctionResult<A, P>>> {

        let mut results = vec![FunctionResult::Error; batch.len()];
        let mut tx_index = batch.len() - 1;
        let storage = SharedStorage{ ptr: self.storage.as_mut_ptr(), size: self.storage.len() };

        while !batch.is_empty() {
            let tx = batch.pop().unwrap();
            // let function = self.functions.get(tx.function as usize).unwrap();
            let function = tx.function;
            match unsafe { function.execute(tx, storage) } {
                Another(generated_tx) => {
                    batch.push(generated_tx);
                },
                result => {
                    results[tx_index] = result;
                    if tx_index != 0 {
                        tx_index -= 1;
                    }
                    continue;
                }
            }
        }

        return Ok(results);
    }
}