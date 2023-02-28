use serde::{Serialize, Deserialize};

#[derive(Debug, Serialize, Deserialize, Copy, Clone)]
pub enum TransactionType {
    Transfer,
    SmartContract,
}