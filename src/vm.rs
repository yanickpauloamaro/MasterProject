use anyhow::{Context, Error, Result};
use async_trait::async_trait;
use std::time::Duration;
use either::{Either, Left, Right};
use tokio::sync::mpsc::{channel, Receiver, Sender};
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
                self.dispatch(start, &mut backlog).await?;
                // println!("Done dispatching");
            }

            let (mut processed, mut conflicts) = self.collect().await?;
            // println!("Collected results: processed {}, conflict {}, backlog length {}", processed.len(), conflicts.len(), backlog.len());

            to_process -= processed.len();
            results.append(&mut processed);
            backlog.append(&mut conflicts);
        }
    }

    async fn dispatch(&mut self, start: Instant, backlog: &mut Vec<Transaction>) -> anyhow::Result<()>;

    async fn collect(&mut self) -> anyhow::Result<(Vec<ExecutionResult>, Vec<Transaction>)>;
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

    // fn find_conflicts(&mut self, batch: Vec<Transaction>) -> (Vec<Transaction>, Vec<Transaction>) {
    //     return batch.into_iter().partition(|tx| !self.conflict(tx));
    //
    //     // let mut tasks = Vec::with_capacity(batch.len());
    //     // let mut conflicts = vec!();
    //     //
    //     // for tx in batch.into_iter() {
    //     //     if self.conflict(&tx) {
    //     //         conflicts.push(tx);
    //     //     } else {
    //     //         tasks.push(tx);
    //     //     }
    //     // }
    //     //
    //     // return (tasks, conflicts);
    // }

    // fn conflict(&mut self, tx: &Transaction) -> bool {
    //     return false;
    // }
}

// pub struct BasicVM {
//     tx_jobs: Vec<Sender<(Instant, Vec<Transaction>)>>,
//     rx_results: Receiver<(Vec<ExecutionResult>, Vec<Transaction>)>
// }
//
// impl BasicVM {
//      pub fn new(
//         tx_jobs: Vec<Sender<(Instant, Vec<Transaction>)>>,
//         rx_results: Receiver<(Vec<ExecutionResult>, Vec<Transaction>)>
//     ) -> Self {
//         return BasicVM{ tx_jobs, rx_results };
//     }
//
//     pub async fn execute(&mut self, mut backlog: Vec<Transaction>) -> anyhow::Result<(Vec<ExecutionResult>, Duration)> {
//
//         let mut to_process = backlog.len();
//         let mut results = Vec::with_capacity(to_process);
//         let start = Instant::now();
//
//         loop {
//             if to_process == 0 {
//                 let duration = start.elapsed();
//                 return Ok((results, duration));
//             }
//
//             self.dispatch(start, &mut backlog).await?;
//
//             let (mut processed, mut conflicts) = self.collect().await?;
//
//             to_process -= processed.len();
//             results.append(&mut processed);
//             backlog.append(&mut conflicts);
//         }
//     }
//
//     async fn dispatch(&mut self, start: Instant, backlog: &mut Vec<Transaction>) -> anyhow::Result<()> {
//
//         for (i, tx_job) in self.tx_jobs.iter_mut().enumerate() {
//
//             let partition_size = backlog.partition_point(|tx| tx.from == i as u64);
//             if partition_size > 0 {
//                 let batch: Vec<Transaction> = backlog.drain(..partition_size).collect();
//                 tx_job.send((start, batch)).await
//                     .context("Unable to send job to worker")?;
//             }
//         }
//
//         if backlog.len() > 0 {
//             return Err(anyhow::anyhow!("Not all jobs where assigned to a worker!"));
//         }
//
//         return Ok(());
//     }
//
//     // async fn dispatch(&mut self, start: Instant, input: &mut Vec<Transaction>) -> Result<()> {
//     //
//     //     let mut backlog: Vec<Transaction> = input.drain(0..input.len()).collect();
//     //     for (i, tx_job) in self.tx_jobs.iter_mut().enumerate() {
//     //         let (batch, rest) = backlog.into_iter().partition(|tx| tx.from == i as u64);
//     //         backlog = rest;
//     //         if batch.len() > 0 {
//     //             tx_job.send((start, batch)).await
//     //                 .context("Unable to send job to worker")?;
//     //         }
//     //     }
//     //
//     //     if backlog.len() > 0 {
//     //         return Err(anyhow::anyhow!("Not all jobs where assigned to a worker!"));
//     //     }
//     //     return Ok(());
//     // }
//
//     async fn collect(&mut self) -> anyhow::Result<(Vec<ExecutionResult>, Vec<Transaction>)> {
//
//         let collect_error: Error = anyhow::anyhow!("Unable to receive results from workers");
//
//         // Another option would be to wait for all the workers to give their results
//         return self.rx_results.recv().await
//             .ok_or(collect_error);
//     }
// }
