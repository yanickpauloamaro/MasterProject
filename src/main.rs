extern crate hwloc;
extern crate anyhow;
extern crate tokio;
extern crate either;

use testbench::config::Config;
use testbench::benchmark::{BasicWorkload, Benchmark, TransactionLoop};
use anyhow::{Context, Result};
// use testbench::wip::benchmark_rate;

#[tokio::main]
async fn main() -> Result<()>{
    println!("Hello, world!");

    let config = Config::new("config_single_batch.json")
        .context("Unable to create benchmark config")?;

    let _ = BasicWorkload::run(config, 1).await;
    // let _ = TransactionLoop::run(config, 1).await;

    println!("See you, world!");

    Ok(())
}