extern crate hwloc;
extern crate anyhow;
extern crate tokio;
extern crate either;

// use crate::benchmark::benchmark;
// use crate::benchmark::test;
// use crate::config::Config;

use testbench::config::Config;
use testbench::benchmark::benchmark;

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

//
// pub fn create_batch(batch_size: usize) {
//     // const NB_ITER: u32 = 100;
//     let mut batch = Vec::with_capacity(batch_size);
//     for _ in 0..batch_size {
//         let tx = wip::Transaction{
//             from: 0,
//             to: 1,
//             amount: 42,
//         };
//         batch.push(tx);
//     }
// }

// mod test {
//     use super::wip;
//
//     pub fn create_batch(batch_size: usize) {
//         // const NB_ITER: u32 = 100;
//         let mut batch = Vec::with_capacity(batch_size);
//         for _ in 0..batch_size {
//             let tx = wip::Transaction{
//                 from: 0,
//                 to: 1,
//                 amount: 42,
//             };
//             batch.push(tx);
//         }
//     }
// }