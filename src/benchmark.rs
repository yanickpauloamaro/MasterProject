use crate::config::Config;
use crate::wip;

use hwloc::{Topology, ObjectType};
// use hwloc::{Topology, CPUBIND_PROCESS, TopologyObject, ObjectType};
use anyhow::{self, Result, Context};
use tokio::sync::mpsc::{channel, Sender};
use tokio::sync::{broadcast, oneshot};

fn check(condition: bool, ctx: &str) -> Result<()> {

    if condition {
        Ok(())
    } else {
        let error_msg = anyhow::anyhow!("Host not compatible. {} not supported", ctx);
        Err(error_msg)
    }
}

fn compatible(topo: &Topology) -> Result<()> {

    check(topo.support().cpu().set_current_process(), "CPU Binding (current process)")?;
    check(topo.support().cpu().set_process(), "CPU Binding (any process)")?;
    check(topo.support().cpu().set_current_thread(), "CPU Binding (current thread)")?;
    check(topo.support().cpu().set_thread(), "CPU Binding (any thread)")?;

    Ok(())
}

pub fn get_nb_cores(topo: &Topology) -> usize {
    let core_depth = topo.depth_or_below_for_type(&ObjectType::Core).unwrap();
    let all_cores = topo.objects_at_depth(core_depth);
    return all_cores.len();
}

pub fn benchmark(config: Config) -> Result<()> {
    let topo = Topology::new();

    // Check compatibility with core pinning
    compatible(&topo)?;

    // Determine number of cores to use
    let nb_cores = get_nb_cores(&topo);
    let nb_nodes  = match config.nb_nodes {
        Some(nb_nodes) if nb_nodes > nb_cores => {
            let error_msg = anyhow::anyhow!(
                "Not enough cores to run benchmark. {} requested but only {} available",
                nb_nodes, nb_cores);
            return Err(error_msg);
        },
        Some(nb_nodes) => nb_nodes,
        None => nb_cores
    };
    let share = config.address_space_size / nb_nodes;

    // Allocate "VM address space" (split per NUMA region?)
    // Create nodes (represent NUMA regions?)
        // Set memory policy so that memory is allocated locally to each core
        // TODO Check how to send transaction to each node
    // Create Backlog
    // Create dispatcher (will send the transactions to the different regions)
    // Create client (will generate transactions)

    const CHANNEL_CAPACITY: usize = 20;
    let (tx_generator, rx_rate) = channel(CHANNEL_CAPACITY);
    let (tx_rate, rx_client) = channel(CHANNEL_CAPACITY);

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

    for w in 0..nb_nodes {
        let (tx_job, rx_job) = channel(CHANNEL_CAPACITY);
        let (tx_result, rx_result) = channel(CHANNEL_CAPACITY);
        let (tx_log, rx_log) = oneshot::channel();

        client.tx_jobs.push(tx_job);
        client.rx_results.push(rx_result);

        let mut worker = wip::Worker{
            rx_job,
            backlog: vec!(),
            log: vec!(),
            tx_result,
            tx_log,
        };

        workers.push(worker);
        rx_logs.push(rx_log);
    }

    let (tx_stop, rx_stop) = broadcast::channel(0);
    // Spawn them in reverse order (workers, client, rate limiter and then generator)
    for w in workers {
        // TODO Spawn each worker on a specific core
        w.spawn(tx_stop.subscribe());
    }
    client.spawn(tx_stop.subscribe());
    rate_limiter.spawn(tx_stop.subscribe());

    println!("Benchmarking!");
    generator.spawn(tx_stop.subscribe());

    tx_stop.send(());
    println!("Done benchmarking");

    for rx_log in rx_logs {
        match rx_log.blocking_recv() {
            Ok(mut log) => {
                // TODO use histogram form hotmic
            },
            Err(e) => {
                eprintln!("Failed to receive log from a worker");
            }
        }
    }

    Ok(())
}