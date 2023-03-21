use std::cmp::max;

use serde::{Deserialize, Serialize};

use crate::vm::{Executor, Jobs};
use crate::vm_a::VMa;
use crate::vm_b::VMb;
use crate::vm_c::VMc;
use crate::wip::{AssignedWorker, Word};
use crate::worker_implementation::{WorkerBStd, WorkerBTokio};

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

pub const UNASSIGNED: AssignedWorker = 0;
pub const CONFLICTING: AssignedWorker = AssignedWorker::MAX-1;
// pub const EXECUTED: AssignedWorker = AssignedWorker::MAX-2;

pub fn assign_workers(
    nb_workers: usize,
    batch: &Jobs,
    address_to_worker: &mut Vec<AssignedWorker>,
    backlog: &mut Jobs
) -> Vec<AssignedWorker> {
    let mut tx_to_worker = vec![UNASSIGNED; batch.len()];
    let mut next_worker = 0 as AssignedWorker;
    let nb_workers = nb_workers as AssignedWorker;

    for (index, tx) in batch.iter().enumerate() {
        let from = tx.from as usize;
        let to = tx.to as usize;

        let worker_from = address_to_worker[from];
        let worker_to = address_to_worker[to];

        if worker_from == UNASSIGNED && worker_to == UNASSIGNED {
            let assigned = next_worker + 1;
            address_to_worker[from] = assigned;
            address_to_worker[to] = assigned;
            tx_to_worker[index] = assigned;
            next_worker = if next_worker == nb_workers - 1 {
                0
            } else {
                next_worker + 1
            };
        } else if worker_from == UNASSIGNED || worker_to == UNASSIGNED {
            let assigned = max(worker_from, worker_to);
            address_to_worker[from] = assigned;
            address_to_worker[to] = assigned;
            tx_to_worker[index] = assigned;

        } else if worker_from == worker_to {
            tx_to_worker[index] = worker_from;

        } else {
            backlog.push(tx.clone());
        }
    }

    return tx_to_worker;
}
