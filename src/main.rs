mod client;
mod benchmark;
mod transaction;
mod node;
mod config;
mod wip;

extern crate hwloc;
extern crate anyhow;
extern crate tokio;
extern crate either;

use crate::benchmark::benchmark;
// use crate::benchmark::test;
use crate::config::Config;

use anyhow::{Result, Context};

#[tokio::main]
async fn main() -> Result<()>{
    println!("Hello, world!");

    let config = Config::new("config.json")
        .context("Unable to create benchmark config")?;

    benchmark(config).await?;
    // test();

    println!("See you, world!");

    Ok(())
}
