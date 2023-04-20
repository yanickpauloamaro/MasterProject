use strum::EnumIter;
use std::mem;
use rand::seq::SliceRandom;
use std::fmt::Debug;
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
    pub function: FunctionAddress,
    // pub addresses: BoundedArray<StaticAddress, MAX_NB_ADDRESSES>,
    // pub params: BoundedArray<FunctionParameter, MAX_NB_PARAMETERS>,
    pub addresses: [StaticAddress; MAX_NB_ADDRESSES],
    pub params: [FunctionParameter; MAX_NB_PARAMETERS],
    // pub nb_addresses: usize,
}

#[derive(Clone, Debug, EnumIter)]
pub enum AtomicFunction {
    Transfer = 0,
    TransferDecrement = 1,
    TransferIncrement = 2,
}

impl AtomicFunction {
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

                    tx.function = TransferIncrement as FunctionAddress;
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
            }
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub enum FunctionResult {
    // Running,
    Success,
    Another(Transaction),
    Error,
    ErrorMsg(&'static str)
}
