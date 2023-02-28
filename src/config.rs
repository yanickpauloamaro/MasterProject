use crate::transaction::TransactionType;
use serde::{Serialize, Deserialize};
use std::fs::{self};
use anyhow::{Result, Context};

#[derive(Debug, Serialize, Deserialize, Copy, Clone)]
pub struct Config {
    pub rate: u32, // in ops/s
    pub batch_size: u32, // in ops
    pub nb_nodes: Option<usize>, // None => all available
    pub address_space_size: usize, // in byte, will be split between NUMA regions
    pub transaction_type: TransactionType,
}

impl Config {
    pub fn new(path: &str) -> Result<Config> {
        let str = fs::read_to_string(path)
            .context("Unable to read config file")?;

        serde_json::from_str(&str)
            .context("Unable to convert json to struct")
    }

    pub fn save(self: &Config, path: &str) -> Result<()> {
        let str = serde_json::to_string_pretty(&self)
            .context("Unable to create json of config")?;

        fs::write(path, str)
            .context("Unable to write config to file")
    }
}