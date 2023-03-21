use std::mem;
use std::sync::Arc;
use std::time::Instant;

use tokio::runtime::{Handle, Runtime};

use crate::{debug, debugging};
use crate::vm::{ExecutionResult, Executor, Jobs};
use crate::vm_utils::{assign_workers, UNASSIGNED, VmMemory};
use crate::wip::{AssignedWorker, Word};
use crate::worker_implementation::{WorkerB, WorkerInput};

//region Parallel VM with background workers =======================================================
pub struct VMb<W> where W: WorkerB + Send + Sized {
    memory: VmMemory,
    nb_workers: usize,
    workers: Vec<W>,

    _runtime_keep_alive: Option<Runtime>,
    _handle: Handle,
}

impl<W: WorkerB + Send + Sized> VMb<W> {
    pub fn new(memory_size: usize, nb_workers: usize, _batch_size: usize) -> anyhow::Result<Self> {
        let memory = VmMemory::new(memory_size);
        let (runtime_keep_alive, handle) = match Handle::try_current() {
            Ok(h) => (None, h),
            Err(_) => {
                let rt = Runtime::new().unwrap();
                let handle = rt.handle().clone();
                (Some(rt), handle)
            },
        };

        let mut workers = Vec::with_capacity(nb_workers);
        for index in 0..nb_workers {
            let worker = W::new(index as AssignedWorker + 1, &handle);
            workers.push(worker);
        }

        let vm = Self{ memory, nb_workers, workers, _runtime_keep_alive: runtime_keep_alive, _handle: handle };
        return Ok(vm);
    }
}

impl<W: WorkerB + Send + Sized> Executor for VMb<W> {
    fn execute(&mut self, mut batch: Jobs) -> anyhow::Result<Vec<ExecutionResult>> {
let total = Instant::now();
        let mut results = Vec::with_capacity(batch.len());
        let mut backlog = Vec::with_capacity(batch.len());
        let mut address_to_worker = vec![UNASSIGNED; self.memory.len()];
// debug!("*** Allocating arrays in {:?}", total.elapsed());

// let mut junk: Vec<Jobs> = vec!();
        loop {
            if batch.is_empty() {
debug!("*** Total took {:?}", total.elapsed());
// println!("{:?}", junk[0].len());
                return Ok(results);
            }

            // Assign jobs to workers ------------------------------------------------------------------
let a = Instant::now();
            address_to_worker.fill(UNASSIGNED);
            let tx_to_worker = assign_workers(
                self.nb_workers,
                &batch,
                &mut address_to_worker,
                &mut backlog
            );
debug!("*** Work assignment took {:?}", a.elapsed());
let start = Instant::now();
            // Start parallel execution ----------------------------------------------------------------
            let batch_arc = Arc::new(batch);
            let tx_to_worker_arc = Arc::new(tx_to_worker);

            for worker in self.workers.iter_mut() {
                let worker_input = WorkerInput {
                    batch: batch_arc.clone(),
                    tx_to_worker: tx_to_worker_arc.clone(),
                    memory: self.memory.get_shared()
                };

                if let Err(e) = worker.send(worker_input) {
                    println!("VM: Failed to send work to worker {}: {:?}", worker.get_index(), e);
                }
            }

            // Collect results -------------------------------------------------------------------------
            for (_worker_index, worker) in self.workers.iter_mut().enumerate() {
                let (mut _accessed, mut worker_output, mut worker_backlog) = worker.receive()?;

                results.append(&mut worker_output);
                backlog.append(&mut worker_backlog);
            }
debug!("*** Parallel execution in {:?}", start.elapsed());
let end = Instant::now();
            // Prepare next iteration --------------------------------------------------------------
            batch = Arc::try_unwrap(batch_arc).unwrap_or(vec!());
            batch.drain(0..).for_each(std::mem::drop);
            mem::swap(&mut batch, &mut backlog);

            // let mut previous_backlog = vec!();
            // mem::swap(&mut backlog, &mut previous_backlog);
            // junk.push(previous_backlog);

            // backlog = vec!();   // !!!
debug!("*** End of loop took {:?}", end.elapsed());
        }
    }

    fn set_memory(&mut self, value: Word) {
        self.memory.set_memory(value);
    }
}
//endregion