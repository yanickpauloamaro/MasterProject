use std::time::Duration;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use async_trait::async_trait;
use anyhow::{Context, Error, Result};
use crate::transaction::{Transaction, TransactionOutput};
use crate::vm::{CHANNEL_CAPACITY, ExecutionResult, VM, VmWorker};

pub struct BasicVM {
    tx_jobs: Vec<Sender<Vec<Transaction>>>,
    rx_results: Receiver<(Vec<ExecutionResult>, Vec<Transaction>)>
}

#[async_trait]
impl VM for BasicVM {
    fn new(nb_workers: usize) -> Self {
        let (tx_result, rx_results) = channel(CHANNEL_CAPACITY);
        let mut tx_jobs = Vec::with_capacity(nb_workers);

        for _ in 0..nb_workers {
            let (tx_job, rx_job) = channel(CHANNEL_CAPACITY);
            BasicWorker::spawn(rx_job, tx_result.clone());
            tx_jobs.push(tx_job);
        }

        return Self {
            tx_jobs,
            rx_results
        };
    }

    async fn prepare(&mut self) {
        println!("Waiting for workers to be ready (2s)");
        // TODO Implement a real way of knowing when the workers are ready...
        tokio::time::sleep(Duration::from_secs(2)).await;
    }

    async fn dispatch(&mut self, backlog: &mut Vec<Transaction>) -> Result<()> {

        for (i, tx_job) in self.tx_jobs.iter_mut().enumerate() {

            let partition_size = backlog.partition_point(|tx| tx.from == i as u64);
            if partition_size > 0 {
                let batch: Vec<Transaction> = backlog.drain(..partition_size).collect();
                // println!("Sending job of size {} to worker {}", batch.len(), i);
                tx_job.send(batch).await
                    .context(format!("Unable to send job to worker"))?;
            }
        }

        if backlog.len() > 0 {
            return Err(anyhow::anyhow!("Not all jobs where assigned to a worker!"));
        }

        return Ok(());
    }

    async fn collect(&mut self) -> Result<(Vec<ExecutionResult>, Vec<Transaction>)> {

        let collect_error: Error = anyhow::anyhow!("Unable to receive results from workers");

        // Another option would be to wait for all the workers to give their results
        return self.rx_results.recv().await
            .ok_or(collect_error);
    }
}

// -------------------------------------------------------------------------------------------------
pub struct BasicWorker {
    rx_jobs: Receiver<Vec<Transaction>>,
    tx_results: Sender<(Vec<ExecutionResult>, Vec<Transaction>)>
}

#[async_trait]
impl VmWorker for BasicWorker {
    fn new(
        rx_jobs: Receiver<Vec<Transaction>>,
        tx_results: Sender<(Vec<ExecutionResult>, Vec<Transaction>)>
    ) -> Self {
        return BasicWorker{ rx_jobs, tx_results};
    }

    async fn get_jobs(&mut self) -> Option<Vec<Transaction>> {
        return self.rx_jobs.recv().await;
    }

    async fn send_results(&mut self, results: Vec<ExecutionResult>, conflicts: Vec<Transaction>) -> Result<()> {
        self.tx_results.send((results, conflicts)).await?;
        return Ok(());
    }

    fn execute(&mut self, tx: Transaction) -> Result<TransactionOutput, Transaction> {
        // TODO If it might conflict with a transaction from another worker, return the transaction
        return Ok(TransactionOutput{ tx });
    }
}

impl BasicWorker {

    pub fn spawn(
        rx_jobs: Receiver<Vec<Transaction>>,
        tx_results: Sender<(Vec<ExecutionResult>, Vec<Transaction>)>
    ) {
        tokio::spawn(async move {
            println!("Spawning basic worker");
            return Self::new(rx_jobs, tx_results)
                .run()
                .await;
        });
    }
}
