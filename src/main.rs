extern crate hwloc;
extern crate anyhow;
extern crate tokio;
extern crate either;

use testbench::config::Config;
use testbench::benchmark::{benchmark, benchmark_workload};
use anyhow::{Context, Result};
// use testbench::wip::benchmark_rate;

#[tokio::main]
async fn main() -> Result<()>{
    println!("Hello, world!");

    // let config = Config::new("config.json")
    //     .context("Unable to create benchmark config")?;
    // benchmark_rate(config).await?;

    // let config = Config::new("config_single_batch.json")
    //     .context("Unable to create benchmark config")?;
    // benchmark_latency(config, 2).await?;

    let config = Config::new("config.json")
        .context("Unable to create benchmark config")?;
    benchmark_workload(config).await?;

    // let duration: u64 = 300;
    // let warmup = Workload::warmup_duration(50_000_000, 64_000, duration)?;
    // println!("Warmup duration for {}s is {:?}", duration, warmup);

    println!("See you, world!");

    Ok(())
}