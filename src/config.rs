use std::fmt;
use std::fs::{self};
use std::time::Duration;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde::de::DeserializeOwned;

use crate::vm_utils::VmType;

pub trait ConfigFile {
    fn new(path: &str) -> Result<Self>
        where Self: Serialize + DeserializeOwned + Sized
    {
        let str = fs::read_to_string(path)
            .context(format!("Unable to read {} file", Self::name()))?;

        serde_json::from_str(&str)
            .context(format!("Unable to convert json to {}", Self::name()))
    }

    fn save<'a>(self: &Self, path: &str) -> Result<()>
        where Self: Serialize + Deserialize<'a> + Sized
    {
        let str = serde_json::to_string_pretty(&self)
            .context(format!("Unable to create json of {}", Self::name()))?;

        fs::write(path, str)
            .context(format!("Unable to write {} to file", Self::name()))
    }

    fn name() -> String;
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct BenchmarkConfig {
    pub vm_types: Vec<VmType>,
    pub nb_cores: Vec<usize>,
    pub batch_sizes: Vec<usize>,
    // pub memory_size: u64, // 2 * batch_size for now
    pub conflict_rates: Vec<f64>,
    pub repetitions: u64,   // For 95% confidence interval
}

impl ConfigFile for BenchmarkConfig{
    fn name() -> String {
        String::from("benchmark config")
    }
}

impl Default for BenchmarkConfig {
    fn default() -> Self {
        return Self {
            vm_types: vec![VmType::A],
            nb_cores: vec![1],
            batch_sizes: vec![128],
            conflict_rates: vec![0.0],
            repetitions: 1,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RunParameter {
    pub vm_type: VmType,
    pub nb_core: usize,
    pub batch_size: usize,
    pub memory_size: usize,
    pub conflict_rate: f64,
    pub repetitions: u64,   // For 95% confidence interval
}

impl RunParameter {
    pub fn new(
        vm_type: VmType,
        nb_core: usize,
        batch_size: usize,
        memory_size: usize,
        conflict_rate: f64,
        repetitions: u64
    ) -> Self {
        return Self{ vm_type, nb_core, batch_size, memory_size, conflict_rate, repetitions }
    }
}

// impl fmt::Display for RunParameter {
//     fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
//         // TODO Add pretty printing
//         write!(f, "Run parameters:\n");
//         write!(f, "todo: {:?}", self.vm_type);
//         write!(f, "todo: {:?}", self.nb_core);
//         write!(f, "todo: {:?}", self.batch_size);
//         write!(f, "todo: {:?}", self.memory_size);
//         write!(f, "todo: {:?}", self.conflict_rate);
//         write!(f, "todo: {:?}", self);
//     }
// }

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct BenchmarkResult {

    pub parameters: RunParameter,

    pub throughput_ci_up: f64,
    pub throughput_micro: f64,
    pub throughput_ci_low: f64,

    pub latency_ci_up: f64,
    pub latency: Duration,
    pub latency_ci_low: f64,

    // TODO Add date?
}

impl ConfigFile for BenchmarkResult {
    fn name() -> String {
        String::from("benchmark result")
    }
}

impl fmt::Display for BenchmarkResult {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let _ = write!(f, "vm_type: {:?} {{\n", self.parameters.vm_type);
        let _ = write!(f, "\tthroughput: {:?} tx/µs\n", self.throughput_micro);
        let _ = write!(f, "\tlatency: {:?}\n", self.latency);
        write!(f, "}}")
    }
}