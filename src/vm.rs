use anyhow::{Context, Result};
use async_trait::async_trait;
use std::time::Duration;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::Instant;

use crate::transaction::{Transaction, TransactionOutput};

pub const CHANNEL_CAPACITY: usize = 200;

#[derive(Debug)]
pub struct ExecutionResult {
    // TODO Remove execution start
    pub output: TransactionOutput,
    pub execution_start: Instant,
    pub execution_end: Instant,
}

#[async_trait]
pub trait VM {
    fn new(nb_workers: usize) -> Self;

    async fn prepare(&mut self);

    async fn execute(&mut self, mut backlog: Vec<Transaction>) -> Result<(Vec<ExecutionResult>, Instant, Duration)> {

        let mut to_process = backlog.len();
        let mut results = Vec::with_capacity(to_process);
        let start = Instant::now();

        loop {
            if to_process == 0 {
                let duration = start.elapsed();
                return Ok((results, start, duration));
            }

            if !backlog.is_empty() {
                self.dispatch(&mut backlog).await?;
                // println!("Done dispatching");
            }

            let (mut processed, mut conflicts) = self.collect().await?;
            // println!("Collected results: processed {}, conflict {}, backlog length {}", processed.len(), conflicts.len(), backlog.len());

            to_process -= processed.len();
            results.append(&mut processed);
            backlog.append(&mut conflicts);
        }
    }

    async fn dispatch(&mut self, backlog: &mut Vec<Transaction>) -> Result<()>;

    async fn collect(&mut self) -> Result<(Vec<ExecutionResult>, Vec<Transaction>)>;
}

#[async_trait]
pub trait VmWorker {

    fn new(
        rx_jobs: Receiver<Vec<Transaction>>,
        tx_results: Sender<(Vec<ExecutionResult>, Vec<Transaction>)>
    ) -> Self;

    async fn get_jobs(&mut self) -> Option<Vec<Transaction>>;
    async fn send_results(&mut self, results: Vec<ExecutionResult>, conflicts: Vec<Transaction>) -> Result<()>;

    async fn run(&mut self) -> Result<()> {
        loop {
            match self.get_jobs().await {
                Some(batch) => {
                    let mut results = Vec::with_capacity(batch.len());
                    let mut conflicts = vec!();

                    for tx in batch {
                        match self.execute(tx) {
                            Ok(output) => {
                                let result = ExecutionResult{
                                    output,
                                    execution_start: Instant::now(), // TODO Will be removed later
                                    execution_end: Instant::now(),
                                };
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
