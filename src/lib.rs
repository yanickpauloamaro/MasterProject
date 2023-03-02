mod client;
pub mod benchmark;
mod transaction;
mod node;
pub mod config;
mod wip;

pub fn create_batch(batch_size: usize) {
    // const NB_ITER: u32 = 100;
    let mut batch = Vec::with_capacity(batch_size);
    for _ in 0..batch_size {
        let tx = wip::Transaction{
            from: 0,
            to: 1,
            amount: 42,
        };
        batch.push(tx);
    }
}