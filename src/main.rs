extern crate hwloc;
extern crate anyhow;
extern crate tokio;
extern crate either;

use testbench::config::Config;
use testbench::benchmark_vm;
use testbench::benchmark::benchmark;
use anyhow::{Result, Context};

#[tokio::main]
async fn main() -> Result<()>{
    println!("Hello, world!");

    let config = Config::new("config.json")
        .context("Unable to create benchmark config")?;

    benchmark(config).await?;
    // benchmark_vm(config, 2).await?;

    println!("See you, world!");

    Ok(())
}