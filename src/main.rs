extern crate hwloc;
extern crate anyhow;
extern crate tokio;
extern crate either;

use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::sync::Arc;
use crossbeam_utils::thread;
use testbench::config::{BenchmarkConfig, Config, ConfigFile};
use testbench::benchmark::{BasicWorkload, Benchmark, benchmarking, ConflictWorkload, ContentionWorkload, TransactionLoop};
use anyhow::{Context, Result};
use tokio::runtime::Runtime;
use testbench::vm_implementation::{VMa, VmMemory, VmType};
use testbench::wip::numa_latency;
use core_affinity;
use testbench::transaction::Transaction;
use testbench::utils::transfer;
use rand::seq::SliceRandom;

fn main() -> Result<()>{
    println!("Hello, world!");

    // let config = Config::new("config_single_batch.json")
    //     .context("Unable to create benchmark config")?;

    // let core_ids = core_affinity::get_core_ids().unwrap();
    // println!("Core ids: {:?}", core_ids);

    // let rt = Runtime::new().unwrap();
    // let _guard = rt.enter();

    // let _ = BasicWorkload::run(config, 1).await;
    // let _ = ContentionWorkload::run(config, 1).await;
    // let _ = TransactionLoop::run(config, 1).await;
    // let _ = ConflictWorkload::run(config, 1);
    benchmarking("benchmark_config.json")?;

    println!("See you, world!");

    Ok(())
}