use crate::config::Config;
use crate::wip;

use hwloc::{Topology, ObjectType};
// use hwloc::{Topology, CPUBIND_PROCESS, TopologyObject, ObjectType};
use anyhow::{self, Result, Context};
use tokio::sync::mpsc::{channel, Sender};
use tokio::sync::{broadcast, oneshot};
use std::thread::sleep;
use std::time::Duration;
use wip::parse_logs;

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

    const CHANNEL_CAPACITY: usize = 200;
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
            logs: vec!(),
            tx_result,
            tx_log,
        };

        workers.push(worker);
        rx_logs.push(rx_log);
    }

    let (tx_stop, rx_stop) = broadcast::channel(1);
    // Spawn them in reverse order (workers, client, rate limiter and then generator)
    for w in workers {
        // TODO Spawn each worker on a specific core
        w.spawn(tx_stop.subscribe());
    }
    client.spawn(tx_stop.subscribe());
    rate_limiter.spawn(tx_stop.subscribe());

    let handle = tokio::spawn(async move {
        let duration = 5;
        println!("Benchmarking for {}s!", duration);
        generator.spawn(tx_stop.subscribe());
        tokio::time::sleep(Duration::from_secs(duration)).await;

        tx_stop.send(());

        println!("\nDone benchmarking! Waiting for tasks to finish (3s)");
        tokio::time::sleep(Duration::from_secs(3)).await;
        let mut processed: u64 = 0;

        println!();
        println!("Collecting logs from workers:");
        for (i, rx) in rx_logs.iter_mut().enumerate() {
            println!("Worker {}:", i);
            match rx.await {
                Ok(block_logs) => {
                    println!("\tProcessed {} blocks", block_logs.len());
                    for (id, creation, logs) in block_logs {
                        // println!("Block {}", id);
                        for completion in logs {
                            // println!("\tA tx took {:?}", completion - creation);
                            processed += 1;
                        }
                    }
                }
                Err(e) => println!("Failed to receive log from worker: {:?}", e)
            }
        }

        println!();
        println!("Processed {} tx in {} s", processed, duration);
        println!("Throughput is {} tx/s", processed/duration);
    });

    handle.await;
    println!();

    // parse_logs(tx_stop, rx_logs);

    // tokio::spawn(aync move {
    // let duration = 10;
    // println!("Benchmarking for {}s!", duration);
    // generator.spawn(tx_stop.subscribe());
    //
    // tokio::time::sleep(Duration::from_secs(duration));
    //
    // tx_stop.send(());
    // println!("Done benchmarking");
    //
    // for rx_log in rx_logs {
    //     match rx_log.blocking_recv() {
    //         Ok(logs) => {
    //             // TODO use histogram form hotmic
    //             for (id, creation, log) in logs {
    //                 println!("Block {}", id);
    //                 for completion in log {
    //                     println!("Took {:?}", completion - creation);
    //                 }
    //             }
    //         },
    //         Err(e) => {
    //             eprintln!("Failed to receive log from a worker");
    //         }
    //     }
    // }
    // });

    Ok(())
}

// pub fn test() {
//     let inputs = vec![Some("First"), None, Some("Last")];
//     // let inputs = vec![0, 1, 2];
//     let (tx, mut rx) = channel(3);
//
//     let (send, mut receive) = channel(2);
//
//     tokio::spawn(async move {
//         for _ in 0..3 {
//             println!("Trying...");
//             tokio::select! {
//                 Some(value) = rx.recv() => {
//                     println!("Received {:?}", value);
//                 },
//                 _ = receive.recv() => {
//                     println!("Quitting");
//                 }
//             }
//         }
//
//         println!("Receiver done");
//
//     });
//
//     tokio::spawn(async move {
//         for input in inputs {
//             println!("Sending {:?}", input);
//             tx.send(input).await;
//         }
//
//         println!("Sender done");
//
//         tokio::time::sleep(Duration::from_secs(3));
//         println!("Asking receiver to stop");
//         send.send(()).await;
//     });
//
//     sleep(Duration::from_secs(6));
//     println!("Done");
// }