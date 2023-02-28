mod client;
mod benchmark;
mod transaction;
mod node;

extern crate hwloc;

use crate::transaction::TransactionType;
use crate::benchmark::{benchmark, Config};

fn main() {
    println!("Hello, world!");

    let config = Config{
        rate: 10,
        batch_size: 10,
        nb_nodes: Some(10),
        address_space_size: 32,
        transaction_type: TransactionType::Transfer
    };

    benchmark(config);

    println!("See you, world!")
}
