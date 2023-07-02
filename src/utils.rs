use std::collections::HashMap;
use std::ops::{Add, Div, Index, IndexMut, Mul, Range};
use std::time::Duration;

// use hwloc::{ObjectType, Topology};
use rand::rngs::StdRng;
use rand::seq::{IteratorRandom, SliceRandom};
use std::fmt::Debug;
use std::iter::Take;
use std::slice::Iter;
use crate::contract::{FunctionParameter, StaticAddress};

use crate::transaction::{Instruction, Transaction, TransactionAddress};
use crate::vm::Jobs;
use crate::wip::Word;

#[macro_export]
macro_rules! debugging {
    () => {
        false
    };
}

#[macro_export]
macro_rules! debug {
    () => {
        if debugging!() {
            println!();
        }
    };
    ($($arg:tt)*) => {{
        if debugging!() {
            println!($($arg)*);
        }
    }};
}

//region BitVector
pub struct BitVector {
    pub inner: [u8; 2],
    size: usize,
}
impl BitVector {
    pub fn new() -> Self {
        Self {
            inner: [0u8; 2],
            size: 0,
        }
    }
    pub fn set(&mut self, mut index: usize) {
        let mut chunk_index = 0;
        while index >= 8 {
            index -= 8;
            chunk_index += 1;
        }

        let mask = 1 << index;
        println!("Setting bit {} of chunk {} using mask {:#b}", index, chunk_index, mask);
        self.inner[chunk_index] = self.inner[chunk_index] | mask;
        self.size += 1;
    }
    pub fn iter(&self) -> IterBitVector<'_> {
        IterBitVector{
            bitvector: &self,
            current_chunk_index: 0,
            current_chunk: self.inner[0],
            current_index: 0,
            done: false,
        }
    }
}

pub struct IterBitVector<'a> {
    pub bitvector: &'a BitVector,
    pub current_chunk_index: usize,
    pub current_chunk: u8,
    pub current_index: usize,
    pub done: bool,
}
impl<'a> Iterator for IterBitVector<'a> {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }

        let mut trailing = self.current_chunk.trailing_zeros() as usize;
        if trailing < 8 {
            let chunk_start = 8 * self.current_chunk_index;
            let found = chunk_start + self.current_index + trailing;
            let jump = trailing + 1;
            // println!("current_index: {}, trailing: {}, chunk: {:#b}, found: {}", self.current_index, trailing, self.current_chunk, found);

            self.current_index += jump;
            self.current_chunk = self.current_chunk >> jump;
            Some(found)
        } else if self.current_chunk_index < self.bitvector.inner.len() - 1 {
            // move to next chunk
            self.current_chunk_index += 1;
            self.current_chunk = self.bitvector.inner[self.current_chunk_index];
            self.current_index = 0;
            self.next()
        } else {
            // done
            self.done = true;
            None
        }
    }
}
//endregion BitVector

//region BoundedArray
#[macro_export]
macro_rules! bounded_array {
    () => (
        $BoundedArray::new()
    );
    ($elem:expr; $n:expr) => (
        BoundedArray::from_elem($elem, $n)
        // [$elem;$n]
    );
    ($($x:expr),+ $(,)?) => (
        BoundedArray::from_slice([$($x),+].as_slice())
        // [$($x),+]
    );
}

#[derive(Debug, Clone, Copy)]
pub struct BoundedArray<T: Sized + Copy + Default + Debug, const COUNT: usize> {
    pub content: [T; COUNT],
    pub occupied: usize
}

impl<const COUNT: usize> BoundedArray<StaticAddress, COUNT> {
    pub fn from_range(range: Range<StaticAddress>) -> Self {
        let mut res = Self::new();
        let range_len = (range.end - range.start) as usize;
        if range_len > COUNT {
            panic!("Size too big");
        }
        res.occupied = range_len;
        let mut i = 0;
        for el in range {
            res.content[i] = el;
            i += 1;
        }
        res
    }

    pub fn from_range_with(head: StaticAddress, range: Range<StaticAddress>) -> Self {
        let mut res = Self::new();
        let range_len = (range.end - range.start) as usize;
        if range_len + 1 > COUNT {
            panic!("Size too big");
        }
        res.occupied = range_len + 1;
        res.content[0] = head;
        let mut i = 1;
        for el in range {
            res.content[i] = el;
            i += 1;
        }
        res
    }
}

impl<T: Sized + Copy + Default + Debug, const COUNT: usize> BoundedArray<T, COUNT> {
    pub fn new() -> Self {
        BoundedArray{
            content: [T::default(); COUNT],
            occupied: 0
        }
    }

    pub fn from_elem(elem: T, n: usize) -> Self {
        let mut res = Self::new();
        if n > COUNT {
            panic!("Size too big");
        }
        res.occupied = n;
        for i in 0..n {
            res.content[i] = elem;
        }
        res
    }

    pub fn from_slice(iter: &[T]) -> Self {
        let mut res = Self::new();
        if iter.len() > COUNT {
            panic!("Size too big");
        }
        res.occupied = iter.len();
        for (i, el) in iter.iter().enumerate() {
            res.content[i] = *el;
        }
        res
    }

    pub fn get_last(&self) -> Option<&T> {
        self.content.get(self.occupied - 1)
    }
    pub fn set_last(&mut self, value: T) {
        self.content[self.occupied - 1] = value;
    }

    pub fn get(&self, i: usize) -> Option<&T> {
        self.content.get(i)
    }

    pub fn get_mut(&mut self, i: usize) -> Option<&mut T> {
        self.content.get_mut(i)
    }

    pub fn set(&mut self, i: usize, el: T) {
        self.content[i] = el;
    }

    pub fn iter(&self) -> Take<Iter<T>>{
        self.content.iter().take(self.occupied)
    }
}

impl<T: Sized + Copy + Default + Debug, const COUNT: usize> Index<usize> for BoundedArray<T, COUNT> {
    type Output = T;

    fn index(&self, index: usize) -> &Self::Output {
        &self.content[index]
    }
}

impl<T: Sized + Copy + Default + Debug, const COUNT: usize> IndexMut<usize> for BoundedArray<T, COUNT> {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        &mut self.content[index]
    }
}
//endregion

//region confidence interval
pub fn mean_ci_str(data: &Vec<Duration>) -> String {
    let (mean, ci) = mean_ci(data);
    format!("{:?} ± {:?}", mean, ci)
}

pub fn mean_ci(data: &Vec<Duration>) -> (Duration, Duration) {
    // Assumes data iid, well defined mean and variance and large N (good enough for N >= 30)
    let n = data.len();
    let eta = 1.96;

    let mean_estimate = data.iter()
        .fold(Duration::from_nanos(0), |a, b| a.add(*b))
        .div(n as u32)
        .as_micros() as i128;

    let mut sum = 0;

    for point in data.iter() {
        let diff = point.as_micros() as i128 - mean_estimate;
        sum += diff * diff;
    }

    let std_estimate = f64::sqrt(sum as f64/n as f64);

    let ci_delta = eta * std_estimate / f64::sqrt(n as f64);

    (Duration::from_micros(mean_estimate as u64), Duration::from_micros(ci_delta as u64))
}
//endregion

pub fn batch_account_creation(batch_size: usize, nb_accounts: usize, amount: u64) -> Vec<Transaction> {
    let mut batch = Vec::with_capacity(batch_size);
    for i in 0..nb_accounts {
        let create = Instruction::CreateAccount(i as u64, amount as Word);
        let tx = Transaction {
            from: 0,
            to: 0,
            instructions: vec!(create),
            // parameters: vec!()
        };
        batch.push(tx);
    }

    return batch;
}

fn transfer(from: TransactionAddress, to: TransactionAddress, amount: u64) -> Vec<Instruction> {
    return vec!(
        Instruction::Decrement(from, amount as Word),
        Instruction::Increment(to, amount as Word),
    );
}

pub fn batch_transfer_loop(batch_size: usize, nb_account: usize) -> Vec<Transaction> {

    let mut batch = Vec::with_capacity(batch_size);
    for account in 0..(nb_account-1) {
        let i = account as u64;
        let next = (i + 1) % nb_account as u64;
        let tx = Transaction {
            from: account as u64,
            to: next,
            instructions: transfer(i, next, i),
            // parameters: vec!()
        };
        batch.push(tx);
    }

    // TODO If we shuffle the batch, we probably get different performance
    return batch;
}

pub fn batch_with_conflicts(batch_size: usize, conflict_rate: f64, mut rng: &mut StdRng) -> Jobs {

    let nb_conflict = (conflict_rate * batch_size as f64).ceil() as usize;

    let mut receiver_occurrences: HashMap<u64, u64> = HashMap::new();
    let mut batch = Vec::with_capacity(batch_size);

    // Create non-conflicting transactions
    for i in 0..batch_size {
        let amount = 2;
        let from = i as u64;
        let mut to = i as u64;

        // Ensure senders and receivers don't conflict. Otherwise, would need to count conflicts
        // between senders and receivers
        to += batch_size as u64;

        receiver_occurrences.insert(to, 1);

        let instructions = transfer(from, to, amount);
        let tx = Transaction{ from, to, instructions};
        batch.push(tx);
    }

    let indices: Vec<usize> = (0..batch_size).collect();

    let mut conflict_counter = 0;
    while conflict_counter < nb_conflict {
        let i = *indices.choose(&mut rng).unwrap();
        let j = *indices.choose(&mut rng).unwrap();

        if batch[i].to != batch[j].to {

            let freq_i = *receiver_occurrences.get(&batch[i].to).unwrap();
            let freq_j = *receiver_occurrences.get(&batch[j].to).unwrap();

            if freq_j != 2 {
                if freq_j == 1 { conflict_counter += 1; }
                if freq_i == 1 { conflict_counter += 1; }

                receiver_occurrences.insert(batch[i].to, freq_i + 1);
                receiver_occurrences.insert(batch[j].to, freq_j - 1);

                batch[j].to = batch[i].to;
            }
        }
    }

    // print_conflict_rate(&batch);

    batch
}

pub fn batch_with_conflicts_new_impl(memory_size: usize, batch_size: usize, conflict_rate: f64, mut rng: &mut StdRng) -> Jobs {

    let nb_conflict = (conflict_rate * batch_size as f64).ceil() as usize;
    let mut addresses: Vec<u64> = (0..memory_size)
            .choose_multiple(&mut rng, 2*batch_size)
            .into_iter().map(|el| el as u64)
            .collect();

    let mut receiver_occurrences: HashMap<u64, u64> = HashMap::new();
    let mut batch = Vec::with_capacity(batch_size);

    // Create non-conflicting transactions
    for _i in 0..batch_size {
        let amount = 2;

        let from = addresses.pop().unwrap();
        let to = addresses.pop().unwrap();

        // Ensure senders and receivers don't conflict. Otherwise, would need to count conflicts
        // between senders and receivers
        // to += batch_size as u64;

        receiver_occurrences.insert(to, 1);

        let instructions = transfer(from, to, amount);
        let tx = Transaction{ from, to, instructions};
        batch.push(tx);
    }

    let indices: Vec<usize> = (0..batch_size).collect();

    let mut conflict_counter = 0;
    while conflict_counter < nb_conflict {
        let i = *indices.choose(&mut rng).unwrap();
        let j = *indices.choose(&mut rng).unwrap();

        if batch[i].to != batch[j].to {

            let freq_i = *receiver_occurrences.get(&batch[i].to).unwrap();
            let freq_j = *receiver_occurrences.get(&batch[j].to).unwrap();

            if freq_j != 2 {
                if freq_j == 1 { conflict_counter += 1; }
                if freq_i == 1 { conflict_counter += 1; }

                receiver_occurrences.insert(batch[i].to, freq_i + 1);
                receiver_occurrences.insert(batch[j].to, freq_j - 1);

                batch[j].to = batch[i].to;
            }
        }
    }

    // print_conflict_rate(&batch);

    batch
}

pub fn print_conflict_rate(batch: &Vec<Transaction>) {

    let mut nb_addresses = 0;

    let mut conflicts = HashMap::new();
    let mut nb_conflicts = 0;
    let mut nb_conflicting_addr = 0;

    for tx in batch.iter() {
        // The 'from' address is always different
        nb_addresses += 1;

        // TODO Make this computation work for arbitrary transfer graphs
        // if addresses.insert(tx.from) {
        //     nb_addresses += 1;
        // }
        // if addresses.insert(tx.to) {
        //     nb_addresses += 1;
        // }

        match conflicts.get_mut(&tx.to) {
            None => {
                conflicts.insert(tx.to, 1);
                nb_addresses += 1;
            },
            Some(occurrence) if *occurrence == 1 => {
                *occurrence += 1;
                nb_conflicts += 2;
                nb_conflicting_addr += 1;
                // println!("** {} is appearing for the 2nd time", tx.to);

            },
            Some(occurrence) => {
                *occurrence += 1;
                nb_conflicts += 1;
                // println!("** {} is appearing for the {}-th time", tx.to, *occurrence);
            },
        }
    }

    // Manual check
    // let mut actual_conflicts = 0;
    // let mut actual_nb_conflicting_addr = 0;
    // for (addr, freq) in conflicts {
    //     // println!("{} appears {} times", addr, freq);
    //     if freq > 1 {
    //         actual_nb_conflicting_addr += 1;
    //         actual_conflicts += freq;
    //         println!("** {} appears {} times", addr, freq);
    //     }
    // }
    // println!("Other calculation: nb conflicts {}", actual_conflicts);
    // println!("Other calculation: nb conflict address {}", actual_nb_conflicting_addr);
    // println!();

    let conflict_rate = (nb_conflicts as f64) / (batch.len() as f64);
    let conflict_addr_rate = (nb_conflicting_addr as f64) / (nb_addresses as f64);

    println!("Nb of conflicts: {}/{}", nb_conflicts, batch.len());
    println!("Conflict rate: {:.2}%",  100.0 * conflict_rate);
    println!("Nb conflicting addresses: {}/{}", nb_conflicting_addr, nb_addresses);
    println!("Ratio of conflicting addresses: {:.2}%",  100.0 * conflict_addr_rate);
    println!();
}

pub fn batch_partitioned(batch_size: usize, nb_partitions: usize) -> Vec<Transaction> {
    let mut batch = Vec::with_capacity(batch_size);
    let chunks = (batch_size / nb_partitions) as u64;

    for i in 0..batch_size {
        let from = i as u64 / chunks;
        let to = ((nb_partitions + i) % (2 * nb_partitions)) as u64;
        let amount = 2 as u64;
        let tx = Transaction{
            from,
            to,
            instructions: transfer(from, to, amount),
        };

        batch.push(tx);
    }

    return batch;
}

pub fn print_throughput(nb_batches: usize, nb_transactions: usize, duration: Duration) {
    println!("Processed {} batches = {} txs in {:?}",
             nb_batches, nb_transactions, duration);

    let micro_throughput = (nb_transactions as f64).div(duration.as_micros() as f64);
    let milli_throughput = micro_throughput.mul(1000.0);
    let throughput = milli_throughput.mul(1000.0);

    println!("Throughput is {} tx/µs", micro_throughput);
    println!("Throughput is {} tx/ms", milli_throughput);
    println!("Throughput is {} tx/s", throughput);
}
