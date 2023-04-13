use std::cmp::max;
use std::collections::VecDeque;
use std::mem;
use std::ops::{Add, BitOr, Div};
use std::time::{Duration, Instant};

use crate::{debug, debugging};
use crate::vm::{ExecutionResult, Executor, Jobs};
use crate::vm_utils::{assign_workers, UNASSIGNED, VmStorage};
use crate::wip::{AssignedWorker, Word};
// use crate::wip::WipTransactionResult::{Error, Pending, Success};
use crate::worker_implementation::WorkerC;

//region Parallel VM crossbeam =====================================================================
pub struct VMc {
    storage: VmStorage,
    nb_workers: usize,
}

impl VMc {
    pub fn new(storage_size: usize, nb_workers: usize, _batch_size: usize) -> anyhow::Result<Self> {

        let storage = VmStorage::new(storage_size);
        let vm = Self{ storage, nb_workers };
        return Ok(vm);
    }
}

impl Executor for VMc {
    fn execute(&mut self, mut batch: Jobs) -> anyhow::Result<Vec<ExecutionResult>> {
let total = Instant::now();
        let mut results = Vec::with_capacity(batch.len());
        let mut backlog = Vec::with_capacity(batch.len());
        let mut address_to_worker = vec![UNASSIGNED; self.storage.len()];

        // return self.execute_rec(results, batch, backlog, address_to_worker);

        loop {
            if batch.is_empty() {
debug!("+++ Total took {:?}\n", total.elapsed());
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
debug!("+++ Work assignment took {:?}", a.elapsed());
let start = Instant::now();
            // Execute in parallel ----------------------------------------------------------------
            WorkerC::crossbeam(
                self.nb_workers,
                &mut results,
                &mut batch,
                &mut backlog,
                &mut self.storage,
                &tx_to_worker,
            )?;
debug!("+++ Parallel execution in {:?}", start.elapsed());
let end = Instant::now();
            // Prepare next iteration --------------------------------------------------------------
            batch.drain(0..).for_each(std::mem::drop);
            mem::swap(&mut batch, &mut backlog);
debug!("+++ End of loop took {:?}", end.elapsed());
        }
    }

    fn set_storage(&mut self, value: Word) {
        self.storage.set_storage(value);
    }
}
//endregion