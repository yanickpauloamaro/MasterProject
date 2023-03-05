use std::io::Write;
use hwloc::Topology;
// use hwloc::{Topology, CPUBIND_PROCESS, TopologyObject, ObjectType};
use anyhow::{self, Context, Result};
use std::time::Duration;
use tokio::time::Instant;
use std::mem;
use num_traits::FromPrimitive;
use crate::basic_vm::BasicVM;
use crate::utils::{compatible, create_batch_partitioned, get_nb_nodes, print_metrics};
use crate::vm::{Batch, VM};
use crate::config::Config;
use crate::transaction::Transaction;

pub async fn benchmark(config: Config, nb_iter: usize) -> Result<()> {

    // Setup ---------------------------------------------------------------------------------------
    let topo = Topology::new();

    // Check compatibility with core pinning
    compatible(&topo)?;

    let nb_nodes = get_nb_nodes(&topo, &config)?;
    // let share = config.address_space_size / nb_nodes;

    let mut vm = BasicVM::new(nb_nodes);
    // vm.prepare().await;

    // Benchmark -----------------------------------------------------------------------------------
    return tokio::spawn(async move {
        println!();
        println!("Benchmarking latency:");
        for iter in 0..nb_iter {
            println!("Iteration {}:", iter);
            let batch = create_batch_partitioned(config.batch_size, nb_nodes);

            let start = Instant::now();
            let result = vm.execute(batch).await?;
            let duration = start.elapsed();

            // Computing latency -----------------------------------------------------------
            print_metrics(vec![(result, start, duration)], duration);
            println!();
        }

        Ok(())
    }).await?;
}

pub async fn benchmark_workload(config: Config) -> Result<()> {

    // Setup ---------------------------------------------------------------------------------------
    let topo = Topology::new();

    // Check compatibility with core pinning
    compatible(&topo)?;

    // Determine number of cores to use
    let nb_nodes = get_nb_nodes(&topo, &config)?;
    // let share = config.address_space_size / nb_nodes;

    let mut workload = Workload::new(
        config.rate as u64,
        config.duration,
        nb_nodes as u64, config.batch_size
    );

    let mut vm = BasicVM::new(nb_nodes);
    // vm.prepare().await;

    return tokio::spawn(async move {
        println!();
        println!("Benchmarking rate {}s:", config.duration);
        let benchmark_start = Instant::now();
        let timer = tokio::time::sleep(Duration::from_secs(config.duration));
        let mut batch_results = Vec::with_capacity(config.batch_size);
        tokio::pin!(timer);

        loop {
            tokio::select! {
                biased;
                () = &mut timer => {
                    println!("** timer reached **");
                    break;
                },
                // Some((creation, batch)) = rx_client.recv() => {
                batch = workload.next_batch() => {
                    if benchmark_start.elapsed() > Duration::from_secs(config.duration) {
                        println!("** took too long **");
                        break;
                    }
                    if batch.is_none() {
                        println!("Finished benchmark early");
                        break;
                    }

                    let start = Instant::now();
                    let result = vm.execute(batch.unwrap()).await?;
                    let duration = start.elapsed();

                    batch_results.push((result, start, duration));
                }
            }
        }

        let total_duration = benchmark_start.elapsed();
        println!("Done benchmarking");
        println!();

        print_metrics(batch_results, total_duration);
        println!();

        Ok(())
    }).await?;
}


pub struct Workload {
    pub rate: u64,
    pub nb_senders: u64,
    pub batch_size: usize,
    pub backlog: Vec<Batch>,
    // pub rate_limiter: RateLimiter
}

impl Workload {
    pub fn new(rate: u64, duration_s: u64, nb_senders: u64, batch_size: usize) -> Workload {

        let start = Instant::now();
        let nb_batches = rate * duration_s / batch_size as u64;
        let mut batches = Vec::with_capacity(nb_batches as usize);
        print!("Preparing {} batches of {} transactions... ", nb_batches, batch_size);
        std::io::stdout().flush().expect("Failed to flush stdout");

        for _ in 0..nb_batches {
            // TODO use randomness
            let batch = create_batch_partitioned(batch_size, nb_senders as usize);
            batches.push(batch);
        }

        let duration = start.elapsed();
        println!("Done. Took {:?}", duration);
        println!();
        // let limiter = RateLimiter::builder()
        //     .initial(100)
        //     .refill(100)
        //     .max(1000)
        //     .interval(Duration::from_millis(250))
        //     .fair(false)
        //     .build();

        return Workload{
            rate,
            nb_senders,
            batch_size,
            backlog: batches,
            // rate_limiter: limiter
        };
    }

    pub async fn next_batch(&mut self) -> Option<Batch> {
        // TODO add a rate limiter?
        return self.backlog.pop();
    }

    pub fn remaining(&self) -> usize {
        return self.backlog.len();
    }

    pub fn warmup_duration(rate: u64, batch_size: usize, benchmark_duration: u64) -> Result<Duration> {
        let err_batches = anyhow::anyhow!("Unable to compute warmup duration");
        let err_duration = anyhow::anyhow!("Unable to compute warmup duration");

        const SINGLE_BATCH_CREATION_DURATION_MICRO_SECS: Duration = Duration::from_micros(200);
        // let start = Instant::now();
        // let batch = create_batch_partitioned(batch_size, 4);
        // let duration = start.elapsed();

        let nb_batches: u32 = u32::from_u64(rate * benchmark_duration / batch_size as u64)
            .ok_or(err_batches)?;

        let mem_size = mem::size_of::<Transaction>();

        println!("Memory used: {} batches * {}B = {}B",
            nb_batches, mem_size,
            nb_batches as usize * mem_size);

        return SINGLE_BATCH_CREATION_DURATION_MICRO_SECS
            .checked_mul(nb_batches)
            .ok_or(err_duration)
            .context("Unable to compute warmup duration");
    }
}
