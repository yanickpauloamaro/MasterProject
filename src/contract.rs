use std::cmp::min;
use strum::EnumIter;
use std::mem;
use rand::seq::SliceRandom;
use std::fmt::Debug;
use crate::applications::{Ballot, BallotPieces};
use crate::utils::BoundedArray;
use crate::vm_utils::SharedStorage;
use crate::wip::Word;

pub type SenderAddress = u32;
pub type FunctionAddress = u32;
pub type StaticAddress = u32;
pub type FunctionParameter = u32;

pub type Round = Vec<Transaction>;
pub type Schedule = Vec<Round>;

pub const MAX_NB_ADDRESSES: usize = 2;
pub const MAX_NB_PARAMETERS: usize = 2;
pub const MAX_TX_SIZE: usize = mem::size_of::<Transaction>();

// TODO Find safe way to have a variable length array?
#[derive(Clone, Debug, Copy)]
pub struct Transaction {
    pub sender: SenderAddress,
    pub function: AtomicFunction,
    pub addresses: BoundedArray<StaticAddress, MAX_NB_ADDRESSES>,
    pub params: BoundedArray<FunctionParameter, MAX_NB_PARAMETERS>,
    // pub addresses: [StaticAddress; MAX_NB_ADDRESSES],
    // pub params: [FunctionParameter; MAX_NB_PARAMETERS],
    // pub nb_addresses: usize,
}

#[repr(u8)]
#[derive(Clone, Copy, Debug, EnumIter)]
pub enum AtomicFunction {
    Transfer = 0,
    // Transfer application split into pieces
    TransferDecrement,
    TransferIncrement,
    Fibonacci,

    Ballot(Ballot),
    // BallotPiece(BallotPieces)

    BestFitStart,
    BestFit,
}

impl AtomicFunction {
    // pub fn index(&self) -> usize {
    //     mem::discriminant(self)
    // }
    pub unsafe fn execute(
        &self,
        mut tx: Transaction,
        mut storage: SharedStorage
    ) -> FunctionResult {
        let _sender = tx.sender;
        let addresses = tx.addresses;
        let params = tx.params;

        use AtomicFunction::*;
        return match self {
            Transfer => {
                let from = addresses[0] as usize;
                let to = addresses[1] as usize;
                let amount = params[0] as Word;

                let balance_from = storage.get(from);
                if balance_from >= amount {
                    *storage.get_mut(from) -= amount;
                    *storage.get_mut(to) += amount;
                    FunctionResult::Success
                } else {
                    println!("*****************");
                    FunctionResult::ErrorMsg("Insufficient funds")
                }
            },
            TransferDecrement => {
                // Example of function that output another function
                let from = addresses[0] as usize;
                let amount = params[0] as Word;
                if storage.get(from) >= amount {
                    *storage.get_mut(from) -= amount;

                    tx.function = TransferIncrement;
                    tx.addresses[0] = params[1] as StaticAddress;

                    FunctionResult::Another(tx)
                } else {
                    println!("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
                    FunctionResult::ErrorMsg("Insufficient funds")
                }
            },
            TransferIncrement => {
                let to = addresses[0] as usize;
                let amount = params[0] as Word;
                *storage.get_mut(to) += amount;
                FunctionResult::Success
            },
            Fibonacci => {
                AtomicFunction::fib(params[0]);
                FunctionResult::Success
            },
            Ballot(piece) => piece.execute(tx, storage),
            BestFitStart => {
                let _addr: Vec<_> = addresses.iter().collect();
                println!("Start range = {:?}", _addr);
                // addresses: [start_addr..end_addr]
                // params: []
                let start_addr = addresses[0];
                let mut max_addr = start_addr;
                let mut max = storage.get(start_addr as usize);

                for addr in addresses.iter() {
                    let value = storage.get(*addr as usize);
                    // println!("\t{}->{}", *addr, value);
                    if value > max {
                        max = storage.get(max_addr as usize);
                        max_addr = *addr;
                    }
                }

                // store max for next tx piece
                tx.params[0] = max as FunctionParameter;

                // Compute next range
                let new_starting_addr = start_addr + MAX_NB_ADDRESSES as StaticAddress;
                let new_end_addr = min(new_starting_addr + MAX_NB_ADDRESSES as StaticAddress - 1, storage.len() as StaticAddress);

                if new_starting_addr as usize >= storage.len() {
                    // Commit
                    return FunctionResult::SuccessValue(max);
                }

                //"lock" the max value
                tx.addresses = BoundedArray::from_range_with(max_addr, new_starting_addr..new_end_addr);

                tx.function = BestFit;
                FunctionResult::Another(tx)
            },
            BestFit => {
                let _addr: Vec<_> = addresses.iter().collect();
                println!("Searching range = {:?}", _addr);
                // addresses: [old_max_addr, start_addr..end_addr]
                // params: [old_max]
                let old_max_addr = addresses[0];
                let old_max = params[0] as Word;
                let start_addr = addresses[1];

                let mut max_addr = old_max_addr;
                let mut max = old_max;

                for addr in addresses.iter() {
                    let value = storage.get(*addr as usize);
                    if value > max {
                        max = storage.get(max_addr as usize);
                        max_addr = *addr;
                    }
                }

                // store max for next tx piece
                tx.params[0] = max as FunctionParameter;

                // Compute next range
                let new_starting_addr = start_addr + MAX_NB_ADDRESSES as StaticAddress - 1;
                let new_end_addr = min(new_starting_addr + MAX_NB_ADDRESSES as StaticAddress - 1, storage.len() as StaticAddress);

                if new_starting_addr as usize >= storage.len() {
                    // Commit
                    return FunctionResult::SuccessValue(max);
                }

                //"lock" the max value
                tx.addresses = BoundedArray::from_range_with(max_addr, new_starting_addr..new_end_addr);

                tx.function = BestFit;
                FunctionResult::Another(tx)
            },
        }
    }

    fn fib(n: FunctionParameter) -> FunctionParameter {
        if n <= 1 {
            1
        } else {
            AtomicFunction::fib(n-1) + AtomicFunction::fib(n-2)
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub enum FunctionResult {
    // Running,
    Success,
    SuccessValue(Word),
    Another(Transaction),
    Error,
    ErrorMsg(&'static str)
}
