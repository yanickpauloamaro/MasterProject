use serde::{Deserialize, Serialize};
use crate::wip::Amount;

#[derive(Debug, Serialize, Deserialize, Copy, Clone)]
pub enum TransactionType {
    Transfer,
    SmartContract,
}

pub type TransactionAddress = u64;


// Transaction for early vm prototypes
#[derive(Debug, Clone)]
pub struct Transaction {
    pub from: TransactionAddress,
    pub to: TransactionAddress,
    pub instructions: Vec<Instruction>,
    // pub parameters: Vec<ContractValue>,
}
// type Variable = u64;

#[derive(Copy, Debug, Clone)]
pub enum Instruction {
    CreateAccount(TransactionAddress, Amount),
    Increment(TransactionAddress, Amount),
    Decrement(TransactionAddress, Amount),
    Read(TransactionAddress),
    Write(TransactionAddress),
    Add(),
    Sub(),
    Push(Amount),
    Pop(),
}

#[derive(Debug)]
pub struct TransactionOutput {
    pub tx: Transaction
}
