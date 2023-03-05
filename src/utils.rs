use std::ops::{Div, Mul};
use std::time::Duration;
use anyhow::Result;
use hwloc::{ObjectType, Topology};
use tokio::time::Instant;

use crate::config::Config;
use crate::transaction::Transaction;
use crate::vm::ExecutionResult;

pub fn check(condition: bool, ctx: &str) -> Result<()> {

    if condition {
        Ok(())
    } else {
        let error_msg = anyhow::anyhow!("Host not compatible. {} not supported", ctx);
        Err(error_msg)
    }
}

pub fn compatible(topo: &Topology) -> Result<()> {

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

pub fn get_nb_nodes(topo: &Topology, config: &Config) -> Result<usize> {
    let nb_cores = get_nb_cores(&topo);
    match config.nb_nodes {
        Some(nb_nodes) if nb_nodes > nb_cores => {
            let error_msg = anyhow::anyhow!(
                "Not enough cores. {} requested but only {} available",
                nb_nodes, nb_cores);
            return Err(error_msg);
        },
        Some(nb_nodes) => Ok(nb_nodes),
        None => Ok(nb_cores)
    }
}

pub fn create_batch_partitioned(batch_size: usize, nb_partitions: usize) -> Vec<Transaction> {
    let mut batch = Vec::with_capacity(batch_size);
    let mut address: u64 = 0;
    let chunks = (batch_size / nb_partitions) as u64;

    for i in 0..batch_size {
        let tx = Transaction{
            from: address / chunks,
            to: nb_partitions as u64,
            amount: i as u64,
        };

        batch.push(tx);
        address += 1;
    }

    return batch;
}

pub fn print_metrics(
    batch_results: Vec<(Vec<ExecutionResult>, Instant, Duration)>,
    total_duration: Duration
) {

    let mut processed: u64 = 0;
    let nb_batches = batch_results.len();
    let mut sum_latency = Duration::from_millis(0);
    let mut min_latency = Duration::MAX;
    let mut max_latency = Duration::from_nanos(0);

    for (results, execution_start, _duration) in batch_results {
        for result in results {
            let latency = result.execution_end - execution_start;
            sum_latency += latency;
            min_latency = min_latency.min(latency);
            max_latency = max_latency.max(latency);
            processed += 1;
        }
    }

    // println!("Batch size: {}, tx processed: {}", batch_size, processed);
    println!("Processed {} batches = {} txs in {:?}", nb_batches, processed, total_duration);

    let micro_throughput = (processed as f64).div(total_duration.as_micros() as f64);
    let milli_throughput = micro_throughput.mul(1000.0);
    let throughput = milli_throughput.mul(1000.0);

    println!("Throughput is {} tx/µs", micro_throughput);
    println!("Throughput is {} tx/ms", milli_throughput);
    println!("Throughput is {} tx/s", throughput);
    println!();
    println!("Min latency is {:?}", min_latency);
    println!("Max latency is {:?}", max_latency);
    // println!("Average latency is {:?} µs", sum_latency.as_micros() / processed as u128);
    println!("Average latency is {:?} ms", sum_latency.as_millis() / processed as u128);
}