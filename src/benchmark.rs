use crate::config::Config;
use crate::{utils, wip};

use hwloc::Topology;
// use hwloc::{Topology, CPUBIND_PROCESS, TopologyObject, ObjectType};
use anyhow::{self, Result, Context};
use tokio::sync::mpsc::channel;
use tokio::sync::{broadcast, oneshot};
use std::time::Duration;
use wip::parse_logs;

// Allocate "VM address space" (split per NUMA region?)
// Create nodes (represent NUMA regions?)
// Set memory policy so that memory is allocated locally to each core
// TODO Check how to send transaction to each node
// Create Backlog
// Create dispatcher (will send the transactions to the different regions)
// Create client (will generate transactions)
pub async fn benchmark(config: Config) -> Result<()> {
    let topo = Topology::new();

    // Check compatibility with core pinning
    utils::compatible(&topo)?;

    // Determine number of cores to use
    let nb_nodes = utils::get_nb_nodes(&topo, &config)?;
    let share = config.address_space_size / nb_nodes;

    const CHANNEL_CAPACITY: usize = 200;
    let (tx_generator, rx_rate) = channel(CHANNEL_CAPACITY);
    let (tx_rate, rx_client) = channel(CHANNEL_CAPACITY);

    // TODO Move initialisation into spawn and use a broadcast variable to trigger the start of the benchmark
    let generator = wip::TransactionGenerator{
        tx: tx_generator
    };
    let rate_limiter = wip::RateLimiter{
        rx: rx_rate,
        tx: tx_rate
    };

    let mut client = wip::Client{
        rx_block: rx_client,
        tx_jobs: vec!(),
        rx_results: vec!(),
    };

    let mut workers: Vec<wip::Worker> = vec!();
    let mut rx_logs: Vec<oneshot::Receiver<Vec<wip::Log>>> = vec!();

    for _ in 0..nb_nodes {
        let (tx_job, rx_job) = channel(CHANNEL_CAPACITY);
        let (tx_result, rx_result) = channel(CHANNEL_CAPACITY);
        let (tx_log, rx_log) = oneshot::channel();

        client.tx_jobs.push(tx_job);
        client.rx_results.push(rx_result);

        let worker = wip::Worker{
            rx_job,
            backlog: vec!(),
            logs: vec!(),
            tx_result,
            tx_log,
        };

        workers.push(worker);
        rx_logs.push(rx_log);
    }

    let (tx_stop, _) = broadcast::channel(1);
    // Spawn them in reverse order (workers, client, rate limiter and then generator)
    for w in workers {
        // TODO Spawn each worker on a specific core
        w.spawn(tx_stop.subscribe());
    }
    client.spawn(tx_stop.subscribe());
    rate_limiter.spawn(tx_stop.subscribe(), config.batch_size, config.rate);
    // tokio::time::sleep(Duration::from_secs(2)).await;


    println!("Benchmarking for {}s!", config.duration);
    generator.spawn(tx_stop.subscribe());
    tokio::time::sleep(Duration::from_secs(config.duration)).await;

    tx_stop.send(()).context("Unable to send stop signal")?;
    println!();
    println!("Done benchmarking! Waiting for tasks to finish (3s)");
    tokio::time::sleep(Duration::from_secs(3)).await;

    parse_logs(
        &config,
        &mut rx_logs,
    ).await;

    Ok(())
}