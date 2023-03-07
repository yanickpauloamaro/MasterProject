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
    pub amount: u64,
}

#[derive(Debug)]
pub struct TransactionOutput {
    pub tx: Transaction
}
