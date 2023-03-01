use tokio::sync::mpsc::{Sender, Receiver};
use tokio::sync::{broadcast, oneshot};
use hotmic::Receiver as MetricReceiver;
use either::{Left, Right, Either};
use std::mem;

type WorkerResult = Either<Vec<Transaction>, ()>;
pub type Log = (u64, u64, Vec<u64>); // block id, block creation time

pub struct Transaction {
    pub block_creation: u64,
    pub completion: u64,
}

pub struct TransactionGenerator {
    /* TODO Check that the generator is fast enough (Throughput > 5 millions/s)
         Otherwise, need to use multiple generators
    */
    pub tx: Sender<Transaction>
}

impl TransactionGenerator {
    pub fn spawn(mut self, mut signal: broadcast::Receiver<()>) {
        tokio::spawn(async move {
            println!("Transaction Generator started");
            loop {
                tokio::select! {
                _ = signal.recv() => {
                    println!("Transaction Generator stopped");
                    return;
                },

                else => {
                    // Generate transactions
                }
            }
            }
        });
    }
}

pub struct RateLimiter {
    pub rx: Receiver<Transaction>,
    pub tx: Sender<Transaction>,
}

impl RateLimiter {
    pub fn spawn(mut self, mut signal: broadcast::Receiver<()>) {
        tokio::spawn(async move {
            println!("$$$ started");
            loop {
                tokio::select! {
                _ = signal.recv() => {
                    println!("$$$ stopped");
                    return;
                },

                else => {
                    // Do work
                }
            }
            }
        });
    }
}

pub struct Client {
    pub rx_block: Receiver<Transaction>,
    pub tx_jobs: Vec<Sender<Transaction>>,
    pub rx_results: Vec<Receiver<WorkerResult>>
}

impl Client {
    pub fn spawn(mut self, mut signal: broadcast::Receiver<()>) {
        tokio::spawn(async move {
            println!("$$$ started");
            loop {
                tokio::select! {
                _ = signal.recv() => {
                    println!("$$$ stopped");
                    return;
                },

                else => {
                    // Do work
                }
            }
            }
        });
    }
}

pub struct Worker {
    pub rx_job: Receiver<Transaction>,
    pub backlog: Vec<Transaction>,
    pub log: Vec<Log>,
    pub tx_result: Sender<WorkerResult>,
    pub tx_log: oneshot::Sender<Vec<Log>>,
}

impl Worker {
    pub fn spawn(mut self, mut signal: broadcast::Receiver<()>) {
        tokio::spawn(async move {
            println!("Worker started");
            loop {
                tokio::select! {
                _ = signal.recv() => {

                    println!("Worker stopped");
                    let nb_logs = self.log.len();
                    self.tx_log.send(mem::take(&mut self.log));
                    return;
                },

                else => {
                    // Do work
                    println!("Worker doing work");
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