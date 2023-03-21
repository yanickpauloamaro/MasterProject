extern crate core;

use std::cmp::max;

pub mod benchmark;
pub mod config;
pub mod transaction;
pub mod wip;
pub mod vm;
pub mod utils;
pub mod vm_utils;
pub mod worker_implementation;
pub mod vm_a;
pub mod vm_b;
pub mod vm_c;

pub fn test(a: u8, b: u8) {
    // let config = BenchmarkConfig::new("benchmark_config.json")
    //     .context("Unable to create benchmark config")?;

    // let a = config.nb_cores[0];
    // let b = config.nb_cores[1];
    let c = max(a, b);
    println!("max is {}", c);
}