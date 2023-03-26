#![allow(unused_variables)]

use std::cmp::{max, min};
use std::collections::{BTreeMap, HashMap};

use hwloc::Topology;
use rand::rngs::StdRng;
use rand::seq::{IteratorRandom, SliceRandom};
use crate::utils::compatible;
use crate::vm::Jobs;
use crate::vm_utils::{UNASSIGNED, VmStorage};

pub const CONFLICT_WIP: u8 = u8::MAX;
pub const DONE: u8 = CONFLICT_WIP - 1;
pub const NONE_WIP: u8 = CONFLICT_WIP - 2;

// type ContractValue = u64;
pub type Amount = u64;
pub type AssignedWorker = u8;
pub type Word = u64;
pub type Address = u64;
pub type Param = u64;

pub fn address_translation(addr: &Address) -> usize {
    return *addr as usize;
}

#[derive(Clone, Debug)]
pub struct WipTransaction {
    pub from: Address,
    pub to: Address,
    pub amount: Amount,
    pub data: Data

    // sequence_number,
    // max_gas,
    // gas_unit_price,
}

impl WipTransaction {
    pub fn transfer(from: Address, to: Address, amount: Amount) -> Self {
        return Self{
            from, to, amount, data: Data::None,
        }
    }

    pub fn call_contract(from: Address, coin_contract: Address, amount: Amount, to: Address) -> Self {
        return Self{
            from,
            to: coin_contract,
            amount: 0,
            data: Data::Parameters(vec!(from, amount, to)),
        }
    }

    pub fn new_coin() -> Self {

        use SegmentInstruction::*;
        let decrement = Segment::new(vec!(
            PushParam(1),
            DecrementFromParam(0),  // Return error in case of underflow
            PushParam(2),
            PushParam(1),
            CallSegment(1), // Uses the stack as the params for the next segment
        ));

        let increment = Segment::new(vec!(
            PushParam(1),
            IncrementFromParam(0),  // Return error in case of underflow
            Return(0),
        ));

        let transfer= Function::new(vec!(decrement, increment));

        return Self{
            from: 0,
            to: 0,
            amount: 0,
            data: Data::NewContract(vec!(transfer)),
        }
    }

    pub fn batch_with_conflicts(memory_size: usize, batch_size: usize, conflict_rate: f64, mut rng: &mut StdRng) -> Vec<WipTransaction> {

        let nb_conflict = (conflict_rate * batch_size as f64).ceil() as usize;
        let mut addresses: Vec<u64> = (0..memory_size)
            .choose_multiple(&mut rng, 2*batch_size)
            .into_iter().map(|el| el as u64)
            .collect();

        let mut receiver_occurrences: HashMap<u64, u64> = HashMap::new();
        let mut batch = Vec::with_capacity(batch_size);

        // Create non-conflicting transactions
        for _i in 0..batch_size {
            let amount = 2;

            let from = addresses.pop().unwrap();
            let to = addresses.pop().unwrap();

            // Ensure senders and receivers don't conflict. Otherwise, would need to count conflicts
            // between senders and receivers
            // to += batch_size as u64;

            receiver_occurrences.insert(to, 1);

            let tx = WipTransaction::transfer(from, to, amount);
            batch.push(tx);
        }

        let indices: Vec<usize> = (0..batch_size).collect();

        let mut conflict_counter = 0;
        while conflict_counter < nb_conflict {
            let i = *indices.choose(&mut rng).unwrap();
            let j = *indices.choose(&mut rng).unwrap();

            if batch[i].to != batch[j].to {

                let freq_i = *receiver_occurrences.get(&batch[i].to).unwrap();
                let freq_j = *receiver_occurrences.get(&batch[j].to).unwrap();

                if freq_j != 2 {
                    if freq_j == 1 { conflict_counter += 1; }
                    if freq_i == 1 { conflict_counter += 1; }

                    receiver_occurrences.insert(batch[i].to, freq_i + 1);
                    receiver_occurrences.insert(batch[j].to, freq_j - 1);

                    batch[j].to = batch[i].to;
                }
            }
        }

        // print_conflict_rate(&batch);

        batch
    }

    pub fn batch_with_conflicts_contract(memory_size: usize, batch_size: usize, conflict_rate: f64, mut rng: &mut StdRng) -> Vec<WipTransaction> {
        let nb_conflict = (conflict_rate * batch_size as f64).ceil() as usize;
        let mut addresses: Vec<u64> = (0..memory_size)
            .choose_multiple(&mut rng, 2*batch_size)
            .into_iter().map(|el| el as u64)
            .collect();

        let mut receiver_occurrences: HashMap<u64, u64> = HashMap::new();
        let mut batch = Vec::with_capacity(batch_size);

        // Create non-conflicting transactions
        for _i in 0..batch_size {
            let amount = 2;
            let from = addresses.pop().unwrap();
            let to = addresses.pop().unwrap();

            // Ensure senders and receivers don't conflict. Otherwise, would need to count conflicts
            // between senders and receivers
            // to += batch_size as u64;

            receiver_occurrences.insert(to, 1);

            let tx = WipTransaction::call_contract(from, 0, amount, to);
            batch.push(tx);
        }

        let indices: Vec<usize> = (0..batch_size).collect();

        let receiver = |data: &Data| {
            match data {
                Data::Parameters(v) => v[2],
                _ => panic!("All tx should  parameters in this context")
            }
        };

        let mut conflict_counter = 0;
        while conflict_counter < nb_conflict {
            let i = *indices.choose(&mut rng).unwrap();
            let j = *indices.choose(&mut rng).unwrap();

            if receiver(&batch[i].data) != receiver(&batch[j].data) {

                let freq_i = *receiver_occurrences.get(&receiver(&batch[i].data)).unwrap();
                let freq_j = *receiver_occurrences.get(&receiver(&batch[j].data)).unwrap();

                if freq_j != 2 {
                    if freq_j == 1 { conflict_counter += 1; }
                    if freq_i == 1 { conflict_counter += 1; }

                    receiver_occurrences.insert(batch[i].to, freq_i + 1);
                    receiver_occurrences.insert(batch[j].to, freq_j - 1);

                    batch[j].to = batch[i].to;
                }
            }
        }

        // print_conflict_rate(&batch);

        batch
    }
}

pub enum WipTransactionResult {
    Error,
    TransferSuccess,
    Success(usize),
}

#[derive(Clone, Debug)]
pub enum Data {
    None,
    NewContract(Vec<Function>),
    Parameters(Vec<Param>)
}

pub struct Contract {
    pub storage: VmStorage,
    // main <=> functions[0]
    pub functions: Vec<Function>,
}

#[derive(Clone, Debug)]
pub struct Function {
    pub segments: Vec<Segment>,

    // Function prototype
    // pub prototype: Protoype
}

impl Function {
    pub fn new(segments: Vec<Segment>) -> Self {
        return Self{segments};
    }
}

#[derive(Clone, Debug)]
pub struct Segment {
    pub instructions: Vec<SegmentInstruction>,
    // pub accesses: Vec<StorageAccess>
}

impl Segment {

    pub fn new(instructions: Vec<SegmentInstruction>) -> Self {
        return Self{instructions};
    }
    pub fn accesses(&self, params: Vec<Param>) -> BTreeMap<Address, AccessType> {
        let mut accesses: BTreeMap<Address, AccessType> = BTreeMap::new();
        use SegmentInstruction::*;
        for instr in self.instructions.iter() {
            match instr {
                IncrementFromParam(i) => {
                    let address = params[*i];
                    match accesses.get_mut(&address) {
                        Some(mut access) => {
                            *access = max(*access, AccessType::Write);
                        },
                        None => {
                            accesses.insert(address, AccessType::Write);
                        }
                    }
                },
                DecrementFromParam(i) => {
                    let address = params[*i];
                    match accesses.get_mut(&address) {
                        Some(mut access) => {
                            *access = max(*access, AccessType::Write);
                        },
                        None => {
                            accesses.insert(address, AccessType::Write);
                        }
                    }
                }
                _ => {

                }
            }
        }

        return accesses;
    }
}

#[derive(PartialEq, Eq, PartialOrd, Ord, Clone, Copy, Debug)]
pub enum AccessType {
    Read = 0,
    Write = 1
}

pub enum StorageAccess {
    // Accessing using a static address
    Read(Address),
    Write(Address),
    // Accessing using a parameter
    // Write(usize),
    // Read(usize),
}

#[derive(Copy, Clone, Debug)]
pub enum SegmentInstruction {
    PushParam(usize),
    IncrementFromParam(usize),
    DecrementFromParam(usize),
    CallSegment(usize),
    Return(u64),
}

// pub struct Interpreter;
//
// impl Interpreter {
//
// }
//
// pub fn test() -> Contract {
//     /*
//     critical {
//         let from = storage[param0]
//         if from < param2 {
//             return 1
//         } else {
//             from = from - param1
//         }
//     }
//
//     go (param1, param2) critical {
//             to = storage[param2]
//             to = to + param1
//             return 0
//         }
//
//
//     critical {
//         let from = storage[param0]  // load from parameter
//         let amount = param1
//         if from < amount {
//             return 1
//         } else {
//             let from = storage[param0]
//             let amount = param1
//             from = from - amount
//             storage[param0] = from
//         }
//     }
//
//     go (param1, param2) critical {
//             to = storage[param2]
//             amount = param1
//             to = to + amount
//             storage[param2] = to
//             return 0
//         }
//      */
//
//     let decrement = Segment{
//         instructions: vec!(
//             LoadFromParam(0),   // push(storage[param0])
//             PushParam(1), // push(param1)
//             Lt,
//             JumpI(+7),
//             LoadFromParam(0),   // push(storage[param0])
//             PushParam(1), // push(param1)
//             Sub,
//             WriteFromParam(0),
//             ParamPopFront,
//             NextSegment,    // is considered a return statement
//             ReturnErr
//         ),
//         accesses: vec!(
//             WriteParam(0)
//         )
//     };
//
//     let increment = Segment{
//         instructions: vec!(
//             LoadFromParam(0),   // push(storage[param0])
//             PushParam(1), // push(param1)
//             Add,
//             WriteFromParam(0),
//             ReturnSuccess
//         ),
//         accesses: vec!(
//             WriteParam(0)
//         )
//     };
//
//     todo!();
// }

pub fn assign_workers_new_impl(
    nb_workers: usize,
    batch: &Jobs,
    address_to_worker: &mut Vec<AssignedWorker>,
    backlog: &mut Jobs,
    worker_to_tx: &mut Vec<Vec<usize>>
) {
    // let mut tx_to_worker = vec![UNASSIGNED; batch.len()];
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
            // tx_to_worker[index] = assigned;
            worker_to_tx[assigned as usize - 1].push(index);
            next_worker = if next_worker == nb_workers - 1 {
                0
            } else {
                next_worker + 1
            };
        } else if worker_from == UNASSIGNED || worker_to == UNASSIGNED {
            let assigned = max(worker_from, worker_to);
            address_to_worker[from] = assigned;
            address_to_worker[to] = assigned;
            // tx_to_worker[index] = assigned;
            worker_to_tx[assigned as usize - 1].push(index);

        } else if worker_from == worker_to {
            // tx_to_worker[index] = worker_from;
            worker_to_tx[worker_from as usize - 1].push(index);
        } else {
            backlog.push(tx.clone());
        }
    }

    // return worker_to_tx;
}

pub fn assign_workers_new_impl_2(
    nb_workers: usize,
    batch: &Jobs,
    address_to_worker: &mut Vec<AssignedWorker>,
    backlog: &mut Jobs,
    next: &mut Vec<usize>
) -> Vec<usize> {
    // let mut tx_to_worker = vec![UNASSIGNED; batch.len()];
    let mut next_worker = 0 as AssignedWorker;
    let mut head = vec![usize::MAX; nb_workers];
    let mut tail = vec![usize::MAX; nb_workers];
    let nb_workers = nb_workers as AssignedWorker;

    for (tx_index, tx) in batch.iter().enumerate() {
        let from = tx.from as usize;
        let to = tx.to as usize;

        let worker_from = address_to_worker[from];
        let worker_to = address_to_worker[to];

        if worker_from == UNASSIGNED && worker_to == UNASSIGNED {
            let assigned = next_worker + 1;
            address_to_worker[from] = assigned;
            address_to_worker[to] = assigned;

            // TODO store triplet instead of using three arrays
            let worker = next_worker as usize;
            let current = min(tx_index, tail[worker]);
            next[current] = tx_index;
            // tx_to_worker[index] = assigned;
            tail[worker] = tx_index;
            head[worker] = min(tx_index, head[worker]);

            next_worker = if next_worker == nb_workers - 1 {
                0
            } else {
                next_worker + 1
            };
        } else if worker_from == UNASSIGNED || worker_to == UNASSIGNED {
            let assigned = max(worker_from, worker_to);
            address_to_worker[from] = assigned;
            address_to_worker[to] = assigned;
            // // tx_to_worker[index] = assigned;
            let worker = assigned as usize - 1;
            let current = min(tx_index, tail[worker]);
            next[current] = tx_index;
            // tx_to_worker[index] = assigned;
            tail[worker] = tx_index;
            head[worker] = min(tx_index, head[worker]);

        } else if worker_from == worker_to {
            // tx_to_worker[index] = worker_from;
            // assignment_linked_list[worker_from as usize - 1].push(tx_index);
            let worker = worker_from as usize - 1;
            let current = min(tx_index, tail[worker]);
            next[current] = tx_index;
            // tx_to_worker[index] = assigned;
            tail[worker] = tx_index;
            head[worker] = min(tx_index, head[worker]);
        } else {
            backlog.push(tx.clone());
        }
    }

    return head;
}
// =================================================================================================

pub fn numa_latency() {
    print!("Checking compatibility with cpu binding...");
    let topo = Topology::new();
    if let Err(e) = compatible(&topo) {
        println!("\n{} not supported", e);
        return;
    }
    println!("Done.\n");

    println!("Displaying NUMA layout:");
    //
    // println!("Enter location of the first thread:");
    // println!("Enter location of the second thread:");
    //
    //
    // println!("Enter the number of iterations (<100):");

    // numa_latency();

    // let length = 10;
    // let mut memory = VmMemory::new(length);
    // println!("Before: {:?}", memory);
    //
    // thread::scope(|s| {
    //     let mut shared1 = memory.get_shared();
    //     let mut shared2 = memory.get_shared();
    //
    //     s.spawn(move |_| {
    //         for i in 0..length.clone() {
    //             if i % 2 == 0 {
    //                 shared1.set(i, (2 * i) as u64);
    //             }
    //         }
    //     });
    //
    //     s.spawn(move |_| {
    //         for i in 0..length.clone() {
    //             if i % 2 == 1 {
    //                 shared2.set(i, i as u64);
    //             }
    //         }
    //     });
    //
    // }).unwrap();
    //
    // println!("After: {:?}", memory);

    // for i in 0..100 {
    //     println!("")
    // }
}

