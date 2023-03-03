use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Copy, Clone)]
pub enum TransactionType {
    Transfer,
    SmartContract,
}

#[derive(Debug)]
pub struct Transaction {
    pub from: u64,
    pub to: u64,
    pub amount: u64,
}

#[derive(Debug)]
pub struct TransactionOutput {
    pub tx: Transaction
}
