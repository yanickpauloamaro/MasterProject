extern crate anyhow;
extern crate either;
extern crate tokio;

use std::time::Instant;

use anyhow::{anyhow, Result};
use futures::executor::block_on;
use testbench::benchmark::TestBench;
use testbench::micro_benchmark::{all_numa_latencies, micro_scheduling, bench_hashmaps, bench_hashmaps_clear};

//#[tokio::main(flavor = "multi_thread", worker_threads = 8)]
// #[tokio::main(flavor = "current_thread")]
// async fn main() -> Result<()> {
fn main() -> Result<()> {

    let total = Instant::now();
    block_on(async {
        TestBench::benchmark("benchmark_config.json").await?;
        anyhow::Ok(())
    })?;

    // micro_scheduling();
    // bench_hashmaps(100, 10, 65536/8);
    // bench_hashmaps_clear(100, 10, 65536/8);

    // block_on(async {
    //     let from_power = 2;
    //     // let to_power = 18;
    //     let to_power = 5;
    //     let nb_cores = 4;
    //
    //     match all_numa_latencies(nb_cores, from_power, to_power).await {
    //         Ok(_) => {},
    //         Err(e) => eprintln!("Failed to run micro benchmark: {:?}", e)
    //     }
    //     anyhow::Ok(())
    // })?;

    println!("main took {:.2?}", total.elapsed());

    Ok(())
}