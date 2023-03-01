use tokio::sync::mpsc::{Sender, Receiver};
use tokio::sync::{broadcast, oneshot};
// use hotmic::Receiver as MetricReceiver;
use either::{Left, Right, Either};
use std::time::Duration;
use std::mem;
use tokio::time::Instant;
use std::future::Future;

type WorkerResult = Either<Vec<Transaction>, ()>;
pub type Log = (u64, Instant, Vec<Instant>); // block id, block creation time

const BLOCK_SIZE: usize = 64 * 1024;

pub fn parse_logs(
    stop_signal: broadcast::Sender<()>,
    rx_logs: Vec<oneshot::Receiver<Vec<Log>>>
) {

    tokio::spawn(async move {
        let duration = 20;
        println!("Benchmarking for {}s!", duration);
        // generator.spawn(tx_stop.subscribe());
        tokio::time::sleep(Duration::from_secs(duration));

        stop_signal.send(());

        println!("Done benchmarking");

        for rx in rx_logs {
            match rx.await {
                Ok(logs) => {
                    for (id, creation, log) in logs {
                        println!("Block {}", id);
                        for completion in log {
                            println!("Took {:?}", completion - creation);
                        }
                    }
                },
                Err(e) => println!("Failed to receive log from worker: {:?}", e)
            }
        }
    });
}

#[derive(Debug)]
pub struct Transaction {
    // pub block_creation: u64,
    // pub completion: u64,
    pub from: u64,
    pub to: u64,
    pub amount: u64,
}

pub struct TransactionGenerator {
    /* TODO Check that the generator is fast enough (Throughput > 5 millions/s)
         Otherwise, need to use multiple generators
    */
    pub tx: Sender<Transaction>
}

async fn trigger(interval: &mut tokio::time::Interval) {
    async {}.await;
    // interval.tick().await;
}

impl TransactionGenerator {
    pub fn spawn(mut self, mut signal: broadcast::Receiver<()>) {
        tokio::spawn(async move {
            println!("<Transaction generator started");
            let mut interval = tokio::time::interval(Duration::from_millis(100));

            loop {
                // println!("\tEntering generator loop");
                tokio::select! {
                    biased;
                    _ = signal.recv() => {
                        println!(">Transaction generator stopped");
                        return;
                    },

                    // _ = interval.tick() => {
                    // _ = async {} => {
                    _ = trigger(&mut interval) => {
                        // Generate transactions
                        let tx = Transaction{
                            from: 0,
                            to: 1,
                            amount: 42,
                        };
                        // let block: Vec<Transaction> = (0..BLOCK_SIZE).map(|_| tx).collect();

                        if let Err(e) = self.tx.send(tx).await {
                            eprintln!("Failed to send tx to rate limiter");
                        }
                    }
                }
            }
        });
    }
}

pub struct RateLimiter {
    pub rx: Receiver<Transaction>,
    pub tx: Sender<(Instant, Vec<Transaction>)>,
}

impl RateLimiter {
    pub fn spawn(mut self, mut signal: broadcast::Receiver<()>) {
        let mut acc: Vec<Transaction> = Vec::with_capacity(BLOCK_SIZE);

        tokio::spawn(async move {
            println!("<Rate limiter started");

            loop {

                tokio::select! {
                _ = signal.recv() => {
                    println!(">Rate limiter stopped");
                    return;
                },

                Some(tx) = self.rx.recv() => {
                    // if acc.len() < BLOCK_SIZE {
                    //     acc.push(tx);
                    // } else {
                    //     let mut block = Vec::with_capacity(BLOCK_SIZE);
                    //     mem::swap(&mut block, &mut acc);
                    //     self.tx.send(block);
                    // }
                    let block = vec![tx];
                    let creation = Instant::now();
                    if let Err(e) = self.tx.send((creation, block)).await {
                        eprintln!("Failed to send tx to client");
                    }
                },
            }
            }
        });
    }
}

pub struct Client {
    pub rx_block: Receiver<(Instant, Vec<Transaction>)>,
    pub tx_jobs: Vec<Sender<(Instant, Vec<Transaction>)>>,
    pub rx_results: Vec<Receiver<WorkerResult>>
}

impl Client {
    pub fn spawn(mut self, mut signal: broadcast::Receiver<()>) {
        tokio::spawn(async move {
            println!("<Client started");
            let mut i = 0;
            let nb_workers = self.tx_jobs.len();

            loop {
                tokio::select! {
                _ = signal.recv() => {
                    println!(">Client stopped");
                    return;
                },

                Some((creation, block)) = self.rx_block.recv() => {
                    // TODO Dispatch to the different workers
                    // println!("Dispatching work");
                    if let Err(e) = self.tx_jobs[i % nb_workers].send((creation, block)).await {
                        eprintln!("Failed to send jobs to worker");
                    }

                    i += 1;
                }
            }
            }
        });
    }
}

pub struct Worker {
    pub rx_job: Receiver<(Instant, Vec<Transaction>)>,
    pub backlog: Vec<Transaction>,
    pub logs: Vec<Log>,
    pub tx_result: Sender<WorkerResult>,
    pub tx_log: oneshot::Sender<Vec<Log>>,
}

impl Worker {
    pub fn spawn(mut self, mut signal: broadcast::Receiver<()>) {
        tokio::spawn(async move {
            println!("<Worker started");
            let mut i = 0;
            loop {
                tokio::select! {
                _ = signal.recv() => {

                    println!(">Worker stopped");
                    let nb_logs = self.logs.len();
                    if let Err(e) = self.tx_log.send(mem::take(&mut self.logs)) {
                            eprintln!("Failed to send logs to benchmark");
                        }
                    return;
                },

                Some((creation, block)) = self.rx_job.recv() => {
                    // TODO optimise by pre allocating some space in either log or backlog?
                    // let backlog = vec!();
                    let mut log = vec!();
                    for tx in block {
                        // println!("Working on {:?}", tx);
                        // tokio::time::sleep(Duration::from_millis(100)).await;
                        let completion = Instant::now();
                        log.push(completion);
                    }
                    self.logs.push((i, creation, log));
                    i += 1;
                }
            }
            }
        });
    }
}

// impl Worker {
    // fn new() {
    //     let receiver = Receiver::builder().build();
    //     let sink = receiver.get_sink();
    //
    //     let start = sink.clock().start();
    //     // thread::sleep(Duration::from_millis(10));
    //     let end = sink.clock().end();
    //
    //     // This would just set the timing:
    //     sink.update_timing("db.gizmo_query", start, end);
    // }
// }