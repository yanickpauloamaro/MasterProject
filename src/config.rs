use std::fmt;
use std::fs::{self};
use std::ops::Div;
use std::time::Duration;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde::de::DeserializeOwned;
use crate::utils::{mean_ci, mean_ci_str};

use crate::vm_utils::VmType;

pub trait ConfigFile {
    fn new(path: &str) -> Result<Self>
        where Self: Serialize + Sized + DeserializeOwned
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
    pub nb_schedulers: Vec<usize>,
    pub nb_executors: Vec<usize>,
    pub batch_sizes: Vec<usize>,
    // pub storage_size: u64, // TODO Add storage size as a config parameter, 2 * batch_size for now
    pub workloads: Vec<String>,
    pub repetitions: u64,   // For 95% confidence interval
    pub warmup: u64,
    pub seed: Option<u64>,
    pub graph: bool,
    pub mapping: String
}

impl ConfigFile for BenchmarkConfig {
    fn name() -> String {
        String::from("benchmark config")
    }
}

impl Default for BenchmarkConfig {
    fn default() -> Self {
        return Self {
            vm_types: vec![VmType::A],
            nb_schedulers: vec![0],
            nb_executors: vec![1],
            batch_sizes: vec![128],
            workloads: vec![String::from("Transfer(0.0)")],
            // workloads: vec!["Transfer(0.0)"],
            repetitions: 1,
            warmup: 1,
            seed: Some(42),
            graph: false,
            mapping: String::from("a"),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RunParameter {
    pub vm_type: VmType,
    pub nb_schedulers: usize,
    pub nb_executors: usize,
    pub batch_size: usize,
    pub storage_size: usize,
    pub workload: String,
    pub repetitions: u64,   // For 95% confidence interval
    pub warmup: u64,
    pub seed: Option<u64>,
    pub graph: bool,
    pub mapping: String
}

impl RunParameter {
    pub fn new(
        vm_type: VmType,
        nb_schedulers: usize,
        nb_executors: usize,
        batch_size: usize,
        storage_size: usize,
        workload: String,
        repetitions: u64,
        warmup: u64,
        seed: Option<u64>,
        graph: bool,
        mapping: String
    ) -> Self {
        return Self{ vm_type, nb_schedulers, nb_executors, batch_size, storage_size, workload, repetitions, warmup, seed, graph, mapping }
    }
}

// impl fmt::Display for RunParameter {
//     fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
//         // TODO Add pretty printing
//         write!(f, "Run parameters:\n");
//         write!(f, "todo: {:?}", self.vm_type);
//         write!(f, "todo: {:?}", self.nb_core);
//         write!(f, "todo: {:?}", self.batch_size);
//         write!(f, "todo: {:?}", self.storage_size);
//         write!(f, "todo: {:?}", self.conflict_rate);
//         write!(f, "todo: {:?}", self);
//     }
// }

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct BenchmarkResult {

    pub parameters: RunParameter,

    pub throughput_ci_up: f64,
    pub throughput_micro: f64,
    pub throughput_ci: f64,
    pub throughput_ci_low: f64,

    pub latency_ci_up: Duration,
    pub latency: Duration,
    pub latency_ci: Duration,
    pub latency_ci_low: Duration,

    // TODO Add date?
    pub latency_breakdown: Option<(String, String)>
}

impl BenchmarkResult {
    pub fn from_latency(run: RunParameter, latency: Vec<Duration>) -> Self {
        let (mean_latency, latency_ci) = mean_ci(&latency);
        let latency_up = mean_latency + latency_ci;
        let latency_low = mean_latency - latency_ci;

        let mean_throughput = (run.batch_size as f64).div(mean_latency.as_micros() as f64);
        // Higher throughput when latency is lower
        let throughput_up = (run.batch_size as f64).div(latency_low.as_micros() as f64);
        let throughput_low = (run.batch_size as f64).div(latency_up.as_micros() as f64);
        let throughput_ci = throughput_up - mean_throughput;

        Self{
            parameters: run,
            throughput_ci_up: throughput_up,
            throughput_micro: mean_throughput,
            throughput_ci,
            throughput_ci_low: throughput_low,
            latency_ci_up: latency_up,
            latency: mean_latency,
            latency_ci,
            latency_ci_low: latency_low,
            latency_breakdown: None
        }
    }

    pub fn from_latency_with_breakdown(run: RunParameter, latency: Vec<Duration>, scheduling_latency: Vec<Duration>, execution_latency: Vec<Duration>) -> Self {
        let mut res = Self::from_latency(run, latency);
        let scheduling_res = mean_ci_str(&scheduling_latency);
        let execution_res = mean_ci_str(&execution_latency);
        res.latency_breakdown = Some((scheduling_res, execution_res));
        res
    }
}

impl ConfigFile for BenchmarkResult {
    fn name() -> String {
        String::from("benchmark result")
    }
}

impl fmt::Display for BenchmarkResult {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let _ = write!(f, "vm_type: {} {{\n", self.parameters.vm_type.name());
        let _ = write!(f, "\tworkload: {}\n", self.parameters.workload);
        let _ = write!(f, "\tnb_schedulers: {}\n", self.parameters.nb_schedulers);
        let _ = write!(f, "\tnb_executors: {}\n", self.parameters.nb_executors);
        let _ = write!(f, "\n",);

        // let _ = write!(f, "\tthroughput: {:.2?} tx/µs\n", self.throughput_micro);
        // let _ = write!(f, "\tthroughput ci: [{:.2?}, {:.2?}]\n", self.throughput_ci_low, self.throughput_ci_up);

        // let _ = write!(f, "\tlatency: {:?}\n", self.latency);
        // let _ = write!(f, "\tlatency ci: [{:?}, {:?}]\n", self.latency_ci_low, self.latency_ci_up);

        let _ = write!(f, "\tthroughput: {:.2} ± {:.2} tx/µs \n", self.throughput_micro, self.throughput_ci);
        let _ = write!(f, "\tlatency: {:?} ± {:?}\n", self.latency, self.latency_ci);
        if let Some((scheduling, execution)) = &self.latency_breakdown {
            let _ = write!(f, "\t\tscheduling: {}\n", scheduling);
            let _ = write!(f, "\t\texecution: {}\n", execution);
        }
        write!(f, "}}")
    }
}