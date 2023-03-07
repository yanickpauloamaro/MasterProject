use std::time::Duration;
use tokio::sync::mpsc::{Receiver, Sender};
use async_trait::async_trait;
use anyhow::{Context, Error, Result};
use crate::transaction::{Transaction, TransactionOutput};
use crate::vm::{ExecutionResult, VM, VmWorker, WorkerPool};

pub struct BasicVM {
    tx_jobs: Vec<Sender<Vec<Transaction>>>,
    rx_results: Receiver<(Vec<ExecutionResult>, Vec<Transaction>)>
}

#[async_trait]
impl VM for BasicVM {
    fn new(nb_workers: usize) -> Self {

        let (tx_jobs, rx_results) = BasicWorker::new_worker_pool(nb_workers);

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

    async fn dispatch(&mut self, backlog: &mut Vec<Transaction>) -> Result<Vec<Transaction>> {

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
            let batch = backlog.drain(..backlog.len()).collect();
            self.tx_jobs[0].send(batch).await
                .context(format!("Unable to send job to worker"))?;
        }

        return Ok(vec!());
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
