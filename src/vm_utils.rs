use std::cmp::max;

use serde::{Deserialize, Serialize};
use thincollections::thin_set::ThinSet;

use crate::config::RunParameter;
use crate::contract::StaticAddress;
use crate::vm::{Executor, Jobs};
use crate::vm_a::VMa;
use crate::vm_b::VMb;
use crate::vm_c::VMc;
use crate::wip::{AssignedWorker, Word};
use crate::worker_implementation::{WorkerBStd, WorkerBTokio};

//region VM Types ==================================================================================
#[derive(Debug, Serialize, Deserialize, Copy, Clone, PartialEq)]
pub enum VmType {
    A,
    BTokio,
    BStd,
    C,
    Sequential,
    ParallelCollect,
    ParallelImmediate,
}

impl VmType {
    pub fn name(&self) -> String {
        match self {
            VmType::A => String::from("VmA"),
            VmType::BTokio => String::from("VmB_Tokio"),
            VmType::BStd => String::from("VmB_Std"),
            VmType::C => String::from("VmC"),
            VmType::Sequential => String::from("Sequential"),
            VmType::ParallelCollect => String::from("ParallelCollect"),
            VmType::ParallelImmediate => String::from("ParallelImmediate"),
        }
    }

    pub fn new(&self) -> bool {
        match self {
            VmType::Sequential => true,
            VmType::ParallelCollect => true,
            VmType::ParallelImmediate => true,
            _ => false
        }
    }
}
//endregion

//region VM Factory ================================================================================
pub struct VmFactory;
impl VmFactory {
    pub fn from(p: &RunParameter) -> Box<dyn Executor> {
        match p.vm_type {
            VmType::A => Box::new(VMa::new(p.storage_size).unwrap()),
            VmType::BTokio => Box::new(VMb::<WorkerBTokio>::new(p. storage_size, p.nb_executors, p.batch_size).unwrap()),
            VmType::BStd => Box::new(VMb::<WorkerBStd>::new(p. storage_size, p.nb_executors, p.batch_size).unwrap()),
            VmType::C => Box::new(VMc::new(p. storage_size, p.nb_executors, p.batch_size).unwrap()),
            _ => todo!()
        }
    }
}
//endregion

//region VM storage ================================================================================
#[derive(Debug)]
pub struct VmStorage {
    pub content: Vec<Word>,
    pub shared: SharedStorage,
}

impl VmStorage {
    pub fn new(size: usize) -> Self {
        let mut content = vec![0 as Word; size];
        let ptr = content.as_mut_ptr();
        let shared = SharedStorage { ptr };

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

    pub fn get_shared(&self) -> SharedStorage {
        return self.shared;
    }

    pub fn set_storage(&mut self, value: Word) {
        self.content.fill(value);
    }

    pub fn total(&self) -> Word {
        self.content.iter().fold(0, |a, b| a + *b)
    }
}

#[derive(Copy, Clone, Debug)]
pub struct SharedStorage {
    pub ptr: *mut Word
}

unsafe impl Send for SharedStorage {}
unsafe impl Sync for SharedStorage {}

impl SharedStorage {
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

    pub fn get_mut(&mut self, index: usize) -> *mut Word {
        unsafe {
            self.ptr.add(index)
        }
    }
}
//endregion

#[derive(Clone, Debug)]
pub struct AddressSet {
    pub inner: ThinSet<StaticAddress>
}
unsafe impl Send for AddressSet {}
// unsafe impl Sync for AddressSet {}

impl AddressSet {
    pub fn with_capacity_and_max(cap: usize, _max: StaticAddress) -> Self {
        let inner = ThinSet::with_capacity(cap);
        return Self { inner };
    }
    pub fn with_capacity(cap: usize) -> Self {
        let inner = ThinSet::with_capacity(cap);
        return Self { inner };
    }
    #[inline]
    pub fn contains(&self, el: StaticAddress) -> bool {
        self.inner.contains(&el)
    }
    #[inline]
    pub fn insert(&mut self, el: StaticAddress) -> bool {
        self.inner.insert(el)
    }
    #[inline]
    pub fn clear(&mut self) {
        self.inner.clear()
    }
}

pub const UNASSIGNED: AssignedWorker = 0;
pub const CONFLICTING: AssignedWorker = AssignedWorker::MAX-1;

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
