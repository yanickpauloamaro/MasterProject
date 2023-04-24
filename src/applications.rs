use std::cmp::min;
use std::fmt;
use std::fmt::{Debug, Formatter};
use std::str::FromStr;

use rand::prelude::{SliceRandom, StdRng};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde::de::{Error, Visitor};
use serde::ser::{SerializeSeq, SerializeTupleStruct};
use thincollections::thin_map::ThinMap;

use crate::utils::BoundedArray;
use crate::bounded_array;
use crate::config::RunParameter;
use crate::contract::{AtomicFunction, FunctionParameter, FunctionResult, MAX_NB_ADDRESSES, SenderAddress, StaticAddress, Transaction};
use crate::vm_utils::SharedStorage;

pub struct Currency {}

impl Currency {

    pub fn transfers_workload(storage_size: usize, batch_size: usize, conflict_rate: f64, mut rng: &mut StdRng) -> Vec<Transaction> {

        let batch: Vec<_> = Workload::transfer_pairs(storage_size, batch_size, conflict_rate, rng)
            .iter()
            .enumerate()
            .map(|(tx_index, pair)| {
                Transaction {
                    sender: pair.0 as SenderAddress,
                    function: AtomicFunction::Transfer,
                    // nb_addresses: 2,
                    addresses: bounded_array![pair.0, pair.1],
                    params: bounded_array!(2, tx_index as FunctionParameter),// [2, tx_index as FunctionParameter]
                }
        }).collect();

        batch
    }

    pub fn split_transfers_workload(storage_size: usize, batch_size: usize, conflict_rate: f64, mut rng: &mut StdRng) -> Vec<Transaction> {

        let batch: Vec<_> = Workload::transfer_pairs(storage_size, batch_size, conflict_rate, rng)
            .iter()
            .enumerate()
            .map(|(tx_index, pair)| {
                Transaction {
                    sender: pair.0 as SenderAddress,
                    function: AtomicFunction::TransferDecrement,
                    addresses: bounded_array![pair.0],
                    // nb_addresses: 1,
                    // addresses: bounded_array![pair.0, pair.1],
                    params: bounded_array!(2, pair.1 as FunctionParameter)
                }
            }).collect();

        batch
    }
}
// #[repr(u8)]
#[derive(Debug, Copy, Clone, Default)]
pub enum Ballot {
    // Voting application
    #[default]
    InitBallot = 0,
    GiveRightToVote,
    Delegate,
    Vote,
    WinningProposal,
    WinnerName,
}

impl Ballot {
    pub fn execute(&self, mut tx: Transaction, mut storage: SharedStorage) -> FunctionResult {
        use Ballot::*;
        match self {
            InitBallot => FunctionResult::Success,
            GiveRightToVote => FunctionResult::Success,
            Delegate => FunctionResult::Success,
            Vote => FunctionResult::Success,
            WinningProposal => FunctionResult::Success,
            WinnerName => FunctionResult::Success,
        }
    }
}

#[derive(Debug, Copy, Clone)]
pub enum BallotPieces {
    // TODO
}

#[derive(Debug, Copy, Clone)]
pub enum Workload {
    Fibonacci(u64),
    Transfer(f64),
    TransferPiece(f64),
    Ballot(u64, f64),
    BestFit
}

impl fmt::Display for Workload {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        use Workload::*;
        match self {
            Fibonacci(n) => write!(f, "Fibonacci({})", n),
            Transfer(rate) => write!(f, "Transfer({:.1}%)", rate),
            TransferPiece(rate) => write!(f, "TransferPiece({:.1}%)", rate),
            Ballot(nb_voters, double_vote_rate) => write!(f, "Ballot({}, {:.1}%)", nb_voters, double_vote_rate),
            BestFit => write!(f, "BestFit"),
            _ => todo!()
        }
    }
}

impl FromStr for Workload {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // TODO Implement proper parsing
        let res = match s {
            "Fibonacci(5)" => Workload::Fibonacci(5),
            "Fibonacci(10)" => Workload::Fibonacci(10),
            "Fibonacci(15)" => Workload::Fibonacci(15),
            "Transfer(0.0)" => Workload::Transfer(0.0),
            "Transfer(0.1)" => Workload::Transfer(0.1),
            "Transfer(0.5)" => Workload::Transfer(0.5),
            "TransferPiece(0.0)" => Workload::TransferPiece(0.0),
            "TransferPiece(0.1)" => Workload::TransferPiece(0.1),
            "TransferPiece(0.5)" => Workload::TransferPiece(0.5),
            "BestFit" => Workload::BestFit,

            // "Ballot(1024, 0.0)" => Workload::Ballot(1024, 0.0),
            // "Ballot(1024, 0.5)" => Workload::Ballot(1024, 0.5),
            // "Ballot(1024, 1.0)" => Workload::Ballot(1024, 1.0),
            //
            // "Ballot(8192, 0.0)" => Workload::Ballot(8192, 0.0),
            // "Ballot(8192, 0.5)" => Workload::Ballot(8192, 0.5),
            // "Ballot(8192, 1.0)" => Workload::Ballot(8192, 1.0),
            //
            // "Ballot(65536, 0.0)" => Workload::Ballot(65536, 0.0),
            // "Ballot(65536, 0.5)" => Workload::Ballot(65536, 0.5),
            // "Ballot(65536, 1.0)" => Workload::Ballot(65536, 1.0),
            _ => todo!()
        };

        Ok(res)
    }
}

impl Workload {

    pub fn new_batch(&self, run: &RunParameter, rng: &mut StdRng) -> Vec<Transaction> {
        use Workload::*;
        match self {
            Fibonacci(n) => {
                (0..run.batch_size).map(|tx_index| {
                    Transaction {
                        sender: tx_index as SenderAddress,
                        function: AtomicFunction::Fibonacci,
                        // nb_addresses: 2,
                        addresses: bounded_array![tx_index as StaticAddress],
                        params: bounded_array!(*n as FunctionParameter, tx_index as FunctionParameter),
                    }
                }).collect()
            },
            Transfer(conflict_rate) => {
                Workload::transfer_pairs(run.storage_size, run.batch_size, *conflict_rate, rng)
                    .iter()
                    .enumerate()
                    .map(|(tx_index, pair)| {
                        Transaction {
                            sender: pair.0 as SenderAddress,
                            function: AtomicFunction::Transfer,
                            // nb_addresses: 2,
                            addresses: bounded_array![pair.0, pair.1],
                            params: bounded_array!(2, tx_index as FunctionParameter),// [2, tx_index as FunctionParameter]
                        }
                    }).collect()
            },
            TransferPiece(conflict_rate) => {
                Workload::transfer_pairs(run.storage_size, run.batch_size, *conflict_rate, rng)
                    .iter()
                    .enumerate()
                    .map(|(tx_index, pair)| {
                        Transaction {
                            sender: pair.0 as SenderAddress,
                            function: AtomicFunction::TransferDecrement,
                            addresses: bounded_array![pair.0],
                            // nb_addresses: 1,
                            // addresses: bounded_array![pair.0, pair.1],
                            params: bounded_array!(2, pair.1 as FunctionParameter)
                        }
                    }).collect()
            },
            Ballot(n, double_vote_rate) => todo!(),
            BestFit => {
                let addresses: Vec<_> = (0..run.storage_size).collect();
                let mut batch = Vec::with_capacity(run.batch_size);
                for sender in 0..run.batch_size {
                    let addr = *addresses.choose(rng).unwrap_or(&0) as StaticAddress;
                    let end_point = min(addr + MAX_NB_ADDRESSES as StaticAddress, run.storage_size as StaticAddress);

                    let tx = Transaction{
                        sender: sender as SenderAddress,
                        function: AtomicFunction::BestFitStart,
                        addresses: BoundedArray::from_range(addr..end_point),
                        // nb_addresses: 1,
                        // addresses: bounded_array![pair.0, pair.1],
                        params: bounded_array!(0)
                    };
                    batch.push(tx)
                }
                batch
            }
            _ => todo!()
        }
    }

    fn transfer_pairs(memory_size: usize, batch_size: usize, conflict_rate: f64, mut rng: &mut StdRng) -> Vec<(StaticAddress, StaticAddress)> {
        let nb_conflict = (conflict_rate * batch_size as f64).ceil() as usize;

        let mut addresses: Vec<StaticAddress> = (0..memory_size).map(|el| el as StaticAddress).collect();
        addresses.shuffle(&mut rng);
        addresses.truncate(2 * batch_size);

        // let mut addresses: Vec<StaticAddress> = (0..memory_size)
        //     .choose_multiple(&mut rng, 2*batch_size)
        //     .into_iter().map(|el| el as StaticAddress)
        //     .collect();

        let mut receiver_occurrences: ThinMap<StaticAddress, u64> = ThinMap::with_capacity(batch_size);
        let mut batch = Vec::with_capacity(batch_size);

        // Create non-conflicting transactions
        for _ in 0..batch_size {
            let from = addresses.pop().unwrap();
            let to = addresses.pop().unwrap();

            // Ensure senders and receivers don't conflict. Otherwise, would need to count conflicts
            // between senders and receivers
            // to += batch_size as u64;

            receiver_occurrences.insert(to, 1);

            batch.push((from, to));
        }

        let indices: Vec<usize> = (0..batch_size).collect();

        let mut conflict_counter = 0;
        while conflict_counter < nb_conflict {
            let i = *indices.choose(&mut rng).unwrap();
            let j = *indices.choose(&mut rng).unwrap();

            if batch[i].1 != batch[j].1 {

                let freq_i = *receiver_occurrences.get(&batch[i].1).unwrap();
                let freq_j = *receiver_occurrences.get(&batch[j].1).unwrap();

                if freq_j != 2 {
                    if freq_j == 1 { conflict_counter += 1; }
                    if freq_i == 1 { conflict_counter += 1; }

                    receiver_occurrences.insert(batch[i].1, freq_i + 1);
                    receiver_occurrences.insert(batch[j].1, freq_j - 1);

                    batch[j].1 = batch[i].1;
                }
            }
        }

        // Workload::print_conflict_rate(&batch);

        batch
    }

    fn print_conflict_rate(batch: &Vec<(StaticAddress, StaticAddress)>) {

        let mut nb_addresses = 0;

        let mut conflicts = ThinMap::new();
        let mut nb_conflicts = 0;
        let mut nb_conflicting_addr = 0;

        for tx in batch.iter() {
            // The 'from' address is always different
            nb_addresses += 1;

            // TODO Make this computation work for arbitrary transfer graphs
            // if addresses.insert(tx.from) {
            //     nb_addresses += 1;
            // }
            // if addresses.insert(tx.addresses[1]) {
            //     nb_addresses += 1;
            // }

            match conflicts.get_mut(&tx.1) {
                None => {
                    conflicts.insert(tx.1, 1);
                    nb_addresses += 1;
                },
                Some(occurrence) if *occurrence == 1 => {
                    *occurrence += 1;
                    nb_conflicts += 2;
                    nb_conflicting_addr += 1;
                    // println!("** {} is appearing for the 2nd time", tx.addresses[1]);

                },
                Some(occurrence) => {
                    *occurrence += 1;
                    nb_conflicts += 1;
                    // println!("** {} is appearing for the {}-th time", tx.addresses[1], *occurrence);
                },
            }
        }

        // Manual check
        // let mut actual_conflicts = 0;
        // let mut actual_nb_conflicting_addr = 0;
        // for (addr, freq) in conflicts {
        //     // println!("{} appears {} times", addr, freq);
        //     if freq > 1 {
        //         actual_nb_conflicting_addr += 1;
        //         actual_conflicts += freq;
        //         println!("** {} appears {} times", addr, freq);
        //     }
        // }
        // println!("Other calculation: nb conflicts {}", actual_conflicts);
        // println!("Other calculation: nb conflict address {}", actual_nb_conflicting_addr);
        // println!();

        let conflict_rate = (nb_conflicts as f64) / (batch.len() as f64);
        let conflict_addr_rate = (nb_conflicting_addr as f64) / (nb_addresses as f64);

        println!("Nb of conflicts: {}/{}", nb_conflicts, batch.len());
        println!("Conflict rate: {:.2}%",  100.0 * conflict_rate);
        println!("Nb conflicting addresses: {}/{}", nb_conflicting_addr, nb_addresses);
        println!("Ratio of conflicting addresses: {:.2}%",  100.0 * conflict_addr_rate);
        println!();
    }
}

impl Serialize for Workload {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error> where S: Serializer, {
        serializer.serialize_str(format!("{:?}", self).as_str())
    }
}

struct WorkloadVisitor;

impl<'de> Visitor<'de> for WorkloadVisitor {
    type Value = Workload;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("A workload used to benchmark a smart contract VM")
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E> where E: Error {
        let res = Workload::from_str(v).unwrap();
        Ok(res)
    }

    fn visit_string<E>(self, v: String) -> Result<Self::Value, E> where E: Error {
        let res = Workload::from_str(v.as_str()).unwrap();
        Ok(res)
    }
}

impl<'de> Deserialize<'de> for Workload {
    fn deserialize<D>(deserializer: D) -> Result<Workload, D::Error> where D: Deserializer<'de>, {
        deserializer.deserialize_string(WorkloadVisitor)
    }
}
