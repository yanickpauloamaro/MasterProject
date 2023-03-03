use anyhow::Result;
use hwloc::Topology;

use crate::config::Config;
use crate::vm::VM;
use crate::basic_vm::BasicVM;
use crate::utils::{print_metrics, create_batch_partitioned};

pub mod benchmark;
pub mod config;
mod transaction;
mod wip;
mod vm;
mod basic_vm;
mod utils;

pub async fn benchmark_vm(config: Config, nb_iter: usize) -> Result<()> {

    // Setup ---------------------------------------------------------------------------------------
    let topo = Topology::new();

    // Check compatibility with core pinning
    utils::compatible(&topo)?;

    let nb_nodes = utils::get_nb_nodes(&topo, &config)?;
    let mut vm = BasicVM::new(nb_nodes);
    // vm.prepare().await;

    // Benchmark -----------------------------------------------------------------------------------
    return tokio::spawn(async move {
        println!("\nBenchmarking...");
        for iter in 0..nb_iter {
            println!("Iteration {}:", iter);
            let batch = create_batch_partitioned(config.batch_size, nb_nodes);

            let (results, execution_start, duration) = vm.execute(batch).await?;

            // Computing latency -----------------------------------------------------------
            print_metrics(config.batch_size, results, execution_start, duration);
            println!();
        }

        Ok(())
    }).await?;
}