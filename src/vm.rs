use anyhow::{Context, Result};
use async_trait::async_trait;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::time::Instant;

use crate::transaction::{Transaction, TransactionOutput};

pub const CHANNEL_CAPACITY: usize = 200;

pub type Batch = Vec<Transaction>;

#[derive(Debug)]
pub struct ExecutionResult {
    pub output: TransactionOutput,
    pub execution_end: Instant,
}

#[async_trait]
pub trait VM {
    fn new(nb_workers: usize) -> Self;

    async fn prepare(&mut self);

    async fn execute(&mut self, mut backlog: Vec<Transaction>) -> Result<Vec<ExecutionResult>> {

        let mut to_process = backlog.len();
        let mut results = Vec::with_capacity(to_process);

        loop {
            if to_process == 0 {
                return Ok(results);
            }

            if !backlog.is_empty() {
                let mut conflicts = self.dispatch(&mut backlog).await?;
                backlog.append(&mut conflicts);
                // println!("Done dispatching");
            }

            let (mut processed, mut jobs) = self.collect().await?;
            // println!("Collected results: processed {}, conflict {}, backlog length {}", processed.len(), conflicts.len(), backlog.len());

            to_process -= processed.len();
            results.append(&mut processed);
            backlog.append(&mut jobs);
        }
    }

    async fn dispatch(&mut self, backlog: &mut Vec<Transaction>) -> Result<Vec<Transaction>>;

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

trait SpawnWorker{
    fn spawn(
        rx_jobs: Receiver<Vec<Transaction>>,
        tx_results: Sender<(Vec<ExecutionResult>, Vec<Transaction>)>
    );
}

impl<W> SpawnWorker for W where W: VmWorker + Send {
    fn spawn(
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

pub trait WorkerPool{
    fn new_worker_pool(nb_workers: usize) -> (
        Vec<Sender<Vec<Transaction>>>,
        Receiver<(Vec<ExecutionResult>, Vec<Transaction>)>
    );
}

impl<W> WorkerPool for W where W: VmWorker + Send {
    fn new_worker_pool(nb_workers: usize) -> (Vec<Sender<Vec<Transaction>>>, Receiver<(Vec<ExecutionResult>, Vec<Transaction>)>) {
        let (tx_result, rx_results) = channel(nb_workers);
        let mut tx_jobs = Vec::with_capacity(nb_workers);

        for _ in 0..nb_workers {
            let (tx_job, rx_job) = channel(CHANNEL_CAPACITY);
            W::spawn(rx_job, tx_result.clone());
            tx_jobs.push(tx_job);
        }

        return (tx_jobs, rx_results);
    }
}