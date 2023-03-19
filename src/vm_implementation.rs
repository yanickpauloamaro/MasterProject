use async_trait::async_trait;
use std::collections::VecDeque;
use std::mem;
use std::sync::Arc;
use std::time::Instant;
use anyhow::{self, Result};
use async_recursion::async_recursion;
use tokio::runtime::{EnterGuard, Handle, Runtime};
use serde::{Serialize, Deserialize};

use crate::transaction::TransactionOutput;
use crate::vm::{CPU, ExecutionResult, Jobs};
use crate::wip::{assign_workers, assign_workers_new_impl, assign_workers_original, AssignedWorker, Executor, NONE_TEST, NONE_WIP, Word};
use crate::worker_implementation::{WorkerB, WorkerBStd, WorkerBTokio, WorkerC, WorkerInput};

//region VM Types ==================================================================================
#[derive(Debug, Serialize, Deserialize, Copy, Clone)]
pub enum VmType {
    A,
    BTokio,
    BStd,
    C
}

impl VmType {
    pub fn name(&self) -> String {
        match self {
            VmType::A => String::from("VmA"),
            VmType::BTokio => String::from("VmB_Tokio"),
            VmType::BStd => String::from("VmB_Std"),
            VmType::C => String::from("VmC"),
        }
    }
}
//endregion

//region VM Factory ================================================================================
pub struct VmFactory;
impl VmFactory {
    pub fn new_vm(tpe: &VmType, memory_size: usize, nb_cores: usize, batch_size: usize) -> Box<dyn Executor> {
        match tpe {
            VmType::A => Box::new(VMa::new(memory_size).unwrap()),
            VmType::BTokio => Box::new(VMb::<WorkerBTokio>::new(memory_size, nb_cores, batch_size).unwrap()),
            VmType::BStd => Box::new(VMb::<WorkerBStd>::new(memory_size, nb_cores, batch_size).unwrap()),
            VmType::C => Box::new(VMc::new(memory_size, nb_cores, batch_size).unwrap()),
        }
    }
}
//endregion

//region VM memory =================================================================================
#[derive(Debug)]
pub struct VmMemory {
    content: Vec<Word>,
    shared: SharedMemory,
}

impl VmMemory {
    pub fn new(size: usize) -> Self {
        let mut content = vec![0 as Word; size];
        let ptr = content.as_mut_ptr();
        let shared = SharedMemory{ ptr };

        return Self{ content, shared};
    }

    pub fn len(&self) -> usize {
        return self.content.len();
    }

    pub fn get(&self, index: usize) -> Word {
        return self.content[index];
    }

    pub fn set(&mut self, index: usize, value: Word) {
        self.content[index] = value;
    }

    pub fn get_shared(&self) -> SharedMemory {
        return self.shared;
    }

    pub fn set_memory(&mut self, value: Word) {
        // let total = self.content.iter().fold(0, |left, right| left + right);
        // println!("Memory total: {}", total);
        self.content.fill(value);
    }
}

#[derive(Copy, Clone, Debug)]
pub struct SharedMemory {
    pub ptr: *mut Word
}

unsafe impl Send for SharedMemory {}

unsafe impl Sync for SharedMemory {}

impl SharedMemory {
    pub fn get(&self, index: usize) -> Word {
        unsafe {
            *self.ptr.add(index)
        }
    }

    pub fn set(&mut self, index: usize, value: Word) {
        unsafe {
            *self.ptr.add(index) = value;
        }
    }
}
//endregion

//region Serial VM =================================================================================
pub struct VMa {
    memory: Vec<Word>
}

impl VMa {
    pub fn new(memory_size: usize) -> anyhow::Result<Self> {
        let memory = (0..memory_size).map(|_| 0 as Word).collect();
        let vm = Self{ memory };
        return Ok(vm);
    }
}

impl Executor for VMa {
    fn execute(&mut self, mut backlog: Jobs) -> Result<Vec<ExecutionResult>> {
let start = Instant::now();
        let mut results = Vec::with_capacity(backlog.len());
        let mut stack = VecDeque::new();
        for tx in backlog {
            for instr in tx.instructions.iter() {
                CPU::execute_from_array(instr, &mut stack, &mut self.memory);
            }
            let output = TransactionOutput{ tx };
            let result = ExecutionResult::todo();
            results.push(result);
            stack.clear();
        }
println!("### Done serial execution in {:?}", start.elapsed());
        return Ok(results);
    }

    fn set_memory(&mut self, value: Word) {
        self.memory.fill(value);
    }
}
//endregion

//region Parallel VM with background workers =======================================================
pub struct VMb<W> where W: WorkerB + Send + Sized {
    memory: VmMemory,
    nb_workers: usize,
    workers: Vec<W>,

    runtime_keep_alive: Option<Runtime>,
    handle: Handle,
}

// TODO use WorkerIndex instead of usize for addr-to-worker and tx-to-worker
pub type WorkerIndex = u8;

impl<W: WorkerB + Send + Sized> VMb<W> {
    pub fn new(memory_size: usize, nb_workers: usize, batch_size: usize) -> anyhow::Result<Self> {
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

        let vm = Self{ memory, nb_workers, workers, runtime_keep_alive, handle };
        return Ok(vm);
    }
}

impl<W: WorkerB + Send + Sized> Executor for VMb<W> {
    fn execute(&mut self, mut batch: Jobs) -> anyhow::Result<Vec<ExecutionResult>> {
let total = Instant::now();
        let mut results = Vec::with_capacity(batch.len());
        let mut backlog = Vec::with_capacity(batch.len());
        let mut address_to_worker = vec![NONE_TEST; self.memory.len()];
println!("*** Allocating arrays in {:?}", total.elapsed());
        // return self.execute_rec(results, batch, backlog, address_to_worker).await;

        loop {
            if batch.is_empty() {
                println!("*** Total took {:?}", total.elapsed());
                return Ok(results);
            }

            // Assign jobs to workers ------------------------------------------------------------------
let a = Instant::now();
            address_to_worker.fill(NONE_TEST);
            let mut tx_to_worker = assign_workers_new_impl(
                self.nb_workers,
                &batch,
                &mut address_to_worker,
                &mut backlog
            );
println!("*** Work assignment took {:?}", a.elapsed());
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
            for (worker_index, worker) in self.workers.iter_mut().enumerate() {
                let (mut accessed, mut worker_output, mut worker_backlog) = worker.receive()?;

                // let z: Vec<usize> = accessed.drain(..16).collect();
                // println!("Worker {} accesses: {:?}", worker_index, z);
                results.append(&mut worker_output);
                backlog.append(&mut worker_backlog);
            }
println!("*** Parallel execution in {:?}", start.elapsed());
let end1 = Instant::now();
let end2 = Instant::now();
            // Prepare next iteration --------------------------------------------------------------
            batch = Arc::try_unwrap(batch_arc).unwrap_or(vec!());
            mem::swap(&mut batch, &mut backlog);
println!("*** Arc unwrap + swap {:?}", end2.elapsed());
            // backlog.clear();    // !!!
            unsafe {
                backlog.set_len(0);
            }
println!("*** End of loop took {:?}", end1.elapsed());
        }
    }

    fn set_memory(&mut self, value: Word) {
        self.memory.set_memory(value);
    }
}
//endregion

//region Parallel VM crossbeam =====================================================================
pub struct VMc {
    memory: VmMemory,
    nb_workers: usize,
}

impl VMc {
    pub fn new(memory_size: usize, nb_workers: usize, batch_size: usize) -> Result<Self> {
        // assert!(nb_workers <= NONE);
        let memory = VmMemory::new(memory_size);
        let vm = Self{ memory, nb_workers };
        return Ok(vm);
    }
}

impl Executor for VMc {
    fn execute(&mut self, mut batch: Jobs) -> Result<Vec<ExecutionResult>> {
        let mut results = Vec::with_capacity(batch.len());
        let mut backlog = Vec::with_capacity(batch.len());
        let mut address_to_worker = vec![NONE_TEST; self.memory.len()];

        // return self.execute_rec(results, batch, backlog, address_to_worker);

        loop {
            if batch.is_empty() {
                return Ok(results);
            }

            // Assign jobs to workers ------------------------------------------------------------------
            address_to_worker.fill(NONE_TEST);
            let mut tx_to_worker = assign_workers(
                self.nb_workers,
                &batch,
                &mut address_to_worker,
                &mut backlog
            );

            // Execute in parallel ----------------------------------------------------------------
            WorkerC::crossbeam(
                self.nb_workers,
                &mut results,
                &mut batch,
                &mut backlog,
                &mut self.memory,
                &tx_to_worker,
            )?;

            // Prepare next iteration --------------------------------------------------------------
            mem::swap(&mut batch, &mut backlog);
            backlog.clear();
        }
    }

    fn set_memory(&mut self, value: Word) {
        self.memory.set_memory(value);
    }
}
//endregion