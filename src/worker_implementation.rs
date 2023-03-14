use tokio::sync::mpsc::{channel as tokio_channel, Receiver as TokioReceiver, Sender as TokioSender};
use std::thread;
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::{Arc, mpsc};
use crossbeam_utils::thread as crossbeam;


use std::collections::VecDeque;
use anyhow::{anyhow, Context, Result};
use crate::transaction::Transaction;
use crate::vm::{CPU, ExecutionResult, Jobs};
use crate::vm_implementation::{SharedMemory, VmMemory};
use crate::wip::Word;

#[derive(Debug)]
pub struct WorkerInput {
    pub batch: Arc<Jobs>,
    pub tx_to_worker: Arc<Vec<usize>>,
    pub memory: SharedMemory,
}

pub type WorkerOutput = (String, Vec<usize>, Vec<ExecutionResult>, Vec<Transaction>);

//region tokio vm worker ===========================================================================
pub struct WorkerBTokio {
    pub index: usize,
    pub tx_job: TokioSender<WorkerInput>,
    pub rx_result: TokioReceiver<WorkerOutput>
}

impl WorkerBTokio {

    pub async fn execute(mut rx_job: TokioReceiver<WorkerInput>, tx_result: TokioSender<WorkerOutput>, index: usize) {
        loop {
            match rx_job.recv().await {
                Some(job) => {
                    let mut shared_memory = job.memory;
                    let batch = job.batch;
                    let tx_to_worker = job.tx_to_worker;

                    let assignment = batch.iter().zip(tx_to_worker.iter());

                    let mut accessed = vec![0; assignment.len()];
                    let worker_name = format!("Worker {}", index);

                    let mut stack: VecDeque<Word> = VecDeque::new();
                    let mut worker_output = vec!();
                    let mut worker_backlog = vec!();

                    for (tx_index, (tx, assigned_worker)) in assignment.enumerate() {
                        if *assigned_worker == index {
                            stack.clear();
                            for instr in tx.instructions.iter() {
                                CPU::execute_from_shared(instr, &mut stack, &mut shared_memory);
                            }
                            let result = ExecutionResult::todo();
                            worker_output.push(result);
                            accessed[tx_index] = 1;
                        }
                    }

                    let result = (worker_name, accessed, worker_output, worker_backlog);
                    if let Err(e) = tx_result.send(result).await {
                        println!("Worker {} can't send result: {}", index, e);
                        return;
                    }
                },
                None => {
                    return;
                }
            }
        }
    }
    pub fn new(
        index: usize,
    ) -> Self {

        // TODO Pin thread to a core
        let (tx_job, mut rx_job) = tokio_channel(1);
        let (mut tx_result, rx_result) = tokio_channel(1);
        tokio::spawn(async move {
            println!("Worker {} spawned (tokio)", index);
            Self::execute(rx_job, tx_result, index).await;
            println!("Worker {} stopped (tokio)", index);
        });

        return Self {
            index,
            tx_job,
            rx_result,
        };
    }

    pub async fn send(&mut self, jobs: WorkerInput) -> Result<()> {
        self.tx_job.send(jobs).await.context("Failed to send job to tokio worker")
    }

    pub async fn receive(&mut self) -> Result<WorkerOutput> {
        self.rx_result.recv().await.context("Failed to receive result from tokio worker")
    }
}
//endregion

//region std vm worker =============================================================================
pub struct WorkerBStd {
    pub index: usize,
    pub tx_job: Sender<WorkerInput>,
    pub rx_result: Receiver<WorkerOutput>
}

impl WorkerBStd {

    pub fn execute(mut rx_job: Receiver<WorkerInput>, tx_result: Sender<WorkerOutput>, index: usize) {
        loop {
            match rx_job.recv() {
                Ok(job) => {
                    let mut shared_memory = job.memory;
                    let batch = job.batch;
                    let tx_to_worker = job.tx_to_worker;

                    let assignment = batch.iter().zip(tx_to_worker.iter());

                    let mut accessed = vec![0; assignment.len()];
                    let worker_name = format!("Worker {}", index);

                    let mut stack: VecDeque<Word> = VecDeque::new();
                    let mut worker_output = vec!();
                    let mut worker_backlog = vec!();

                    for (tx_index, (tx, assigned_worker)) in assignment.enumerate() {
                        if *assigned_worker == index {
                            stack.clear();
                            for instr in tx.instructions.iter() {
                                CPU::execute_from_shared(instr, &mut stack, &mut shared_memory);
                            }
                            let result = ExecutionResult::todo();
                            worker_output.push(result);
                            accessed[tx_index] = 1;
                        }
                    }

                    let result = (worker_name, accessed, worker_output, worker_backlog);
                    if let Err(e) = tx_result.send(result) {
                        println!("Worker {} can't send result: {}", index, e);
                        return;
                    }
                },
                Err(e) => {
                    return;
                }
            }
        }
    }
    pub fn new(
        index: usize,
    ) -> Self {

        // TODO Pin thread to a core using core_affinity
        let (tx_job, mut rx_job) = channel();
        let (mut tx_result, rx_result) = channel();
        thread::spawn(move || {
            println!("Worker {} spawned (std::thread)", index);
            Self::execute(rx_job, tx_result, index);
            println!("Worker {} stopped (std::thread)", index);
        });

        return Self {
            index,
            tx_job,
            rx_result,
        };
    }

    pub async fn send(&mut self, jobs: WorkerInput) -> Result<()> {
        self.tx_job.send(jobs).context("Failed to send")
    }

    pub async fn receive(&mut self) -> Result<WorkerOutput> {
        self.rx_result.recv().context("Failed to receive")
    }
}
//endregion

//region crossbeam worker ==========================================================================
pub struct WorkerC;

impl WorkerC {
    pub fn crossbeam(
        nb_workers: usize,
        results: &mut Vec<ExecutionResult>,
        batch: &mut Jobs,
        backlog: &mut Jobs,
        memory: &mut VmMemory,
        tx_to_worker: &Vec<usize>
    ) -> Result<()>
    {
        let mut execution_errors: Vec<Result<()>> = vec!();
        crossbeam::scope(|s| {
            let mut shared_memory = memory.get_shared();
            let mut handles = Vec::with_capacity(nb_workers);

            for worker_index in 0..nb_workers {
                let assignment = batch.iter().zip(tx_to_worker.iter());

                handles.push(s.spawn(move |_| {
                    let mut accessed = vec![0; assignment.len()];
                    let worker_name = format!("Worker {}", worker_index);

                    let mut stack: VecDeque<Word> = VecDeque::new();
                    let mut worker_output = vec!();
                    let mut worker_backlog = vec!();

                    for (tx_index, (tx, assigned_worker)) in assignment.clone().enumerate() {
                        if *assigned_worker == worker_index {
                            stack.clear();
                            for instr in tx.instructions.iter() {
                                CPU::execute_from_shared(instr, &mut stack, &mut shared_memory);
                            }

                            let result = ExecutionResult::todo();
                            worker_output.push(result);
                            accessed[tx_index] = 1;
                        }
                    }

                    (worker_name, accessed, worker_output, worker_backlog)
                }));
            }

            for handle in handles {
                match handle.join() {
                    Ok((worker_name, accessed, mut worker_output, mut worker_backlog)) => {
                        results.append(&mut worker_output);
                        backlog.append(&mut worker_backlog);
                    },
                    Err(e) => {
                        execution_errors.push(Err(anyhow!("{:?}", e)));
                    }
                }
            }
        });

        if execution_errors.is_empty() {
            return Ok(());
        }

        return Err(anyhow!("Some error occurred during crossbeam execution: {:?}", execution_errors));
    }
}
//endregion
