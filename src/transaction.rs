use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Copy, Clone)]
pub enum TransactionType {
    Transfer,
    SmartContract,
}

pub type TransactionAddress = u64;

#[derive(Debug)]
pub struct Transaction {
    pub from: TransactionAddress,
    pub to: TransactionAddress,
    pub instructions: Vec<Instruction>,
    // pub parameters: Vec<ContractValue>,
}

type ContractValue = u64;
type Amount = u64;
type Variable = u64;

#[derive(Debug)]
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
