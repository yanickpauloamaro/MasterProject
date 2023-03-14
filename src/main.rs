extern crate hwloc;
extern crate anyhow;
extern crate tokio;
extern crate either;

use std::sync::Arc;
use crossbeam_utils::thread;
use testbench::config::Config;
use testbench::benchmark::{BasicWorkload, Benchmark, ConflictWorkload, ContentionWorkload, TransactionLoop};
use anyhow::{Context, Result};
use testbench::vm_implementation::VmMemory;
use testbench::wip::numa_latency;

#[tokio::main]
async fn main() -> Result<()>{
    println!("Hello, world!");

    let config = Config::new("config_single_batch.json")
        .context("Unable to create benchmark config")?;

    // let _ = BasicWorkload::run(config, 1).await;
    // let _ = ContentionWorkload::run(config, 1).await;
    // let _ = TransactionLoop::run(config, 1).await;
    let _ = ConflictWorkload::run(config, 1).await;

    println!("See you, world!");

    Ok(())
}