use std::cell::Cell;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::mem;
use std::time::Duration;
use thincollections::thin_map::ThinMap;

use crate::{auction, d_hash_map, key_value};
use crate::auction::SimpleAuction;
use crate::d_hash_map::{DHashMap, DKey, PiecedOperation, SearchResult};
use crate::d_hash_map::Success::Replaced;
use crate::key_value::{KeyValue, KeyValueOperation, Value};
use crate::vm_utils::SharedStorage;
use crate::wip::Word;

pub type SenderAddress = u32;
pub type FunctionAddress = u32;
pub type StaticAddress = u32;
pub type FunctionParameter = u32;

pub const MAX_NB_ADDRESSES: usize = 2;
pub const MAX_NB_PARAMETERS: usize = 2;

#[derive(Clone, Debug, Copy)]
pub struct Transaction<const ADDRESS_COUNT: usize, const PARAM_COUNT: usize> {
    pub sender: SenderAddress,
    pub function: AtomicFunction,
    pub tx_index: usize,    // Could use u32
    pub addresses: [StaticAddress; ADDRESS_COUNT],
    pub params: [FunctionParameter; PARAM_COUNT],
}

#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AtomicFunction {
    Transfer = 0,
    TransferTest,
    // Transfer application split into pieces
    TransferDecrement,
    TransferIncrement,
    Fibonacci,

    //Key-value application
    KeyValue(KeyValueOperation),

    Auction(auction::Operation),

    DHashMap(d_hash_map::Operation),
    PieceDHashMap(d_hash_map::PiecedOperation),

    // Ballot(Ballot),
    // BallotPiece(BallotPieces)

    BestFitStart,
    BestFit,
}

#[derive(Clone, Debug)]
pub enum FunctionResult<const ADDRESS_COUNT: usize, const PARAM_COUNT: usize> {
    // Running,
    Success,
    SuccessValue(Word),
    Another(Transaction<ADDRESS_COUNT, PARAM_COUNT>),
    Error,
    ErrorMsg(&'static str),
    // TODO Rename all functions, results and errors with qualified names -> i.e. key_value::Result
    KeyValueSuccess(key_value::OperationResult),
    KeyValueError(key_value::KeyValueError),
    Auction(auction::Result),
    DHashMap(d_hash_map::Result<PARAM_COUNT>),
}

impl AtomicFunction {
    // pub fn index(&self) -> usize {
    //     mem::discriminant(self)
    // }
    pub fn all() -> Vec<AtomicFunction> {
        vec![
            AtomicFunction::Transfer,
            AtomicFunction::TransferTest,
            AtomicFunction::TransferDecrement,
            AtomicFunction::TransferIncrement,
            AtomicFunction::Fibonacci,
            AtomicFunction::Auction(auction::Operation::Bid),
            AtomicFunction::Auction(auction::Operation::Withdraw),
            AtomicFunction::Auction(auction::Operation::Close),
            AtomicFunction::KeyValue(KeyValueOperation::Read),
            AtomicFunction::KeyValue(KeyValueOperation::Write),
            AtomicFunction::KeyValue(KeyValueOperation::ReadModifyWrite),
            AtomicFunction::KeyValue(KeyValueOperation::Scan),
            AtomicFunction::KeyValue(KeyValueOperation::Insert),
        ]
    }
    pub unsafe fn execute<const ADDRESS_COUNT: usize, const PARAM_COUNT: usize>(
        &self,
        mut tx: Transaction<ADDRESS_COUNT, PARAM_COUNT>,
        mut storage: SharedStorage
    ) -> FunctionResult<ADDRESS_COUNT, PARAM_COUNT> {
        // use AtomicFunction::*;
        return match self {
            AtomicFunction::Transfer => {
                let from = tx.addresses[0] as usize;
                let to = tx.addresses[1] as usize;
                let amount = tx.params[0] as Word;
                // eprintln!("{} transfers {}$ to {}", from, amount, to);
                let balance_from = storage.get(from);
                if balance_from >= amount {
                    *storage.get_mut(from) -= amount;
                    *storage.get_mut(to) += amount;
                    FunctionResult::Success
                } else {
                    eprintln!("Transfer: Insufficient funds !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
                    FunctionResult::ErrorMsg("Insufficient funds")
                }
            },
            AtomicFunction::TransferTest => {
                let nb_accounts = storage.get(0) as usize;
                let map_start = (storage.ptr.add(1)) as *mut Option<Word>;
                let balances = SharedMap::from_ptr(
                    Cell::new(map_start),
                    nb_accounts,
                    storage.size * mem::size_of::<Word>(),
                );

                let from = tx.addresses[0];
                let to = tx.addresses[1];
                let amount = tx.params[0] as Word;

                let balance_from = balances.get(from).unwrap();
                // eprintln!("Transfer {}$ from {} to {}:", amount, from, to);
                // eprintln!("\tbalance before: from: {:?}, to: {:?}", balances.get(from), balances.get(to));
                if *balance_from >= amount {
                    *(balances.get_mut(from).unwrap()) -= amount;
                    *(balances.get_mut(to).unwrap()) += amount;

                    // eprintln!("\tbalance after: from: {:?}, to: {:?}", balances.get(from), balances.get(to));
                    // eprintln!();
                    FunctionResult::Success
                } else {
                    eprintln!("Transfer: Insufficient funds !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
                    FunctionResult::ErrorMsg("Insufficient funds")
                }
            }
            AtomicFunction::TransferDecrement => {
                // Example of function that output another function
                let from = tx.addresses[0] as usize;
                let to = tx.params[1] as usize;
                let amount = tx.params[0] as Word;

                if storage.get(from) >= amount {
                    *storage.get_mut(from) -= amount;

                    tx.function = AtomicFunction::TransferIncrement;
                    tx.addresses[0] = to as StaticAddress;

                    FunctionResult::Another(tx)
                } else {
                    eprintln!("TransferDecrement: Insufficient funds !!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
                    FunctionResult::ErrorMsg("Insufficient funds")
                }
            },
            AtomicFunction::TransferIncrement => {
                let to = tx.addresses[0] as usize;
                let amount = tx.params[0] as Word;
                *storage.get_mut(to) += amount;
                FunctionResult::Success
            },
            AtomicFunction::Fibonacci => {
                // TODO change to AtomicFunction::Compute(ComputeFunction::Fibonacci)
                AtomicFunction::fib(tx.params[0]);
                FunctionResult::Success
            },
            AtomicFunction::Auction(op) => {

                let auction_address = tx.params[1] as usize;
                let auction_obj_start = storage.ptr.add(auction_address);

                let beneficiary = auction_obj_start.read() as StaticAddress;
                let auction_balance_address = auction_obj_start.add(1).read() as StaticAddress;
                let end_time = auction_obj_start.add(2).read();
                let ended = auction_obj_start.add(3).read() == 0;   // TODO 0 is true?
                let highest_bidder = auction_obj_start.add(4).read() as StaticAddress;
                let highest_bid = auction_obj_start.add(5).read();
                let nb_elem_in_map = auction_obj_start.add(6).read() as usize;
                let map_start = auction_obj_start.add(7) as *mut Option<Word>;

                let pending_returns = SharedMap::from_ptr(
                    Cell::new(map_start),
                    nb_elem_in_map,
                    (storage.size - auction_address - 7) * mem::size_of::<Word>(),
                );
                let mut auction = SimpleAuction {
                    beneficiary,
                    auction_balance_address,
                    end_time,
                    ended,
                    highest_bidder,
                    highest_bid,
                    pending_returns,
                };

                // println!("Retrieved auction object: {:?}", auction);
                use auction::Operation::*;
                let res = match op {
                    Bid => {
                        let bidder = tx.addresses[0];
                        if bidder != tx.sender {
                            eprintln!("{} is trying to bid for {}", tx.sender, bidder);
                            return FunctionResult::ErrorMsg("Cannot bid for someone else");
                        }
                        let new_bid = tx.params[0] as Word;
                        // println!("{} bids {}", bidder, new_bid);
                        auction.bid(bidder, new_bid)
                    },
                    Withdraw => {
                        let recipient = tx.addresses[0];
                        if recipient != tx.sender {
                            return FunctionResult::ErrorMsg("Cannot withdraw someone else's funds");
                        }
                        // println!("{} withdraws", recipient);
                        auction.withdraw(recipient)
                    },
                    Close => {
                        if tx.sender != beneficiary {
                            return FunctionResult::ErrorMsg("Only the beneficiary can end the auction");
                        }
                        // println!("{} closes auction", tx.sender);
                        auction.close_auction()
                    },
                };

                // eprintln!("\t{:?}", res);
                auction_obj_start.add(3).write(if auction.ended { 0 } else { 2 });
                auction_obj_start.add(4).write(auction.highest_bidder as Word);
                auction_obj_start.add(5).write(auction.highest_bid);

                match res {
                    Ok(auction::Success::Transfer(from, to, amount)) => {
                        tx.function = AtomicFunction::Transfer;
                        // TODO Could use negative numbers to represent addresses that don't need to be scheduled
                        // TODO Still need scheduling needs to know how to interpret the addresses when we add ranges
                        tx.addresses[0] = from;
                        tx.addresses[1] = to;
                        tx.params[0] = amount as FunctionParameter;

                        FunctionResult::Another(tx)
                    },
                    err => FunctionResult::Auction(err)
                }
            }
            AtomicFunction::KeyValue(op) => {
                let nb_elem_in_map = storage.get(0) as usize;
                let map_start = (storage.ptr.add(1)) as *mut Option<Value>;
                let shared_map = SharedMap::from_ptr(
                    Cell::new(map_start),
                    nb_elem_in_map,
                    storage.size * mem::size_of::<Word>(),
                );
                let mut key_value = KeyValue { inner_map: shared_map };

                use KeyValueOperation::*;
                let res = match op {
                    Read => {
                        let key = tx.addresses[0] as StaticAddress;
                        key_value.read(key)
                    }
                    Write => {
                        let key = tx.addresses[0] as StaticAddress;
                        let new_value = Value::new(tx.params[0] as u64);
                        key_value.write(key, new_value)
                    }
                    ReadModifyWrite => {
                        let key = tx.addresses[0] as StaticAddress;
                        // todo!(How to represent different read modify functions?)
                        let op = |input: Value| {
                            Value::new(2 * input.content[0])
                        };
                        key_value.read_modify_write(key, op)
                    }
                    Scan => {
                        let from = tx.addresses[0] as StaticAddress;
                        let to = tx.addresses[1] as StaticAddress;
                        key_value.scan(from, to)
                    }
                    Insert => {
                        let key = tx.addresses[0] as StaticAddress;
                        let value = Value::new(tx.params[0] as u64);
                        key_value.insert(key, value)
                    }
                };
                // eprintln!("\tOperation result: {} -> {:?}", tx.params[0], res);
                match res {
                    Ok(success) => FunctionResult::KeyValueSuccess(success),
                    Err(failure) => FunctionResult::KeyValueError(failure)
                }
            }
            AtomicFunction::DHashMap(op) => {

                use d_hash_map::Operation::*;
                match op {
                    Get | Remove | ContainsKey => {
                        tx.function = AtomicFunction::PieceDHashMap(op.corresponding_piece());
                        let request = tx;

                        match request.function.execute(tx, storage) {
                            // Request pieces always return a new tx
                            FunctionResult::Another(actual_op) => {
                                actual_op.function.execute(actual_op, storage)
                            },
                            _ => panic!("Unexpected result from a DHashMap::Operation")
                        }
                    },
                    Insert => {
                        tx.function = AtomicFunction::PieceDHashMap(PiecedOperation::TryInsert);
                        let request = tx;

                        let request_output = request.function.execute(tx, storage);
                        if let FunctionResult::Another(try_insert) = request_output {
                            let try_insert_output = try_insert.function.execute(tx, storage);
                            if let FunctionResult::Another(resize) = try_insert_output {
                                // TryInsert failed and wants to resize
                                let resize_output = resize.function.execute(resize, storage);
                                if let FunctionResult::Another(actual_insert) = resize_output {
                                    let actual_insert_output = actual_insert.function.execute(actual_insert, storage);
                                    if let FunctionResult::Another(_) = actual_insert_output {
                                        // TryInsert after Resize should always be successful
                                        panic!("Unexpected result from a DHashMap::Operation")
                                    } else {
                                        actual_insert_output
                                    }
                                } else {
                                    // Resize should always return a new TryInsert
                                    panic!("Unexpected result from a DHashMap::Operation")
                                }
                            } else {
                                // Successful insert
                                try_insert_output
                            }
                        } else {
                            // Request pieces always return a new tx
                            panic!("Unexpected result from a DHashMap::Operation")
                        }

                        // match request_piece.execute(tx, storage) {
                        //     // Request pieces always return a new tx
                        //     FunctionResult::Another(next_tx) => {
                        //         match next_tx.function.execute(next_tx, storage) {
                        //             FunctionResult::Another(next_tx) => {
                        //                 // TryInsert failed and wants to resize
                        //                 match next_tx.function.execute(next_tx, storage) {
                        //                     FunctionResult::Another(next_tx) => {
                        //
                        //                     },
                        //                     result => result
                        //                 }
                        //             },
                        //             result => result
                        //         }
                        //     },
                        //     _ => panic!("Unexpected result from a DHashMap::Operation")
                        // }
                    },
                }
            },

            AtomicFunction::PieceDHashMap(op) => {

                use d_hash_map::PiecedOperation::*;
                use d_hash_map::Success::*;

                let res = match op {
                    InsertRequest | RemoveRequest | GetRequest | HasRequest => {
                        // println!("------------ Compute Hash ---------------");
                        let key = tx.params[0];
                        let hash = d_hash_map::DHashMap::compute_hash(key);

                        let nb_buckets = storage.get(0);
                        let bucket_capacity = storage.get(1) as usize;
                        let bucket_index = (hash % (nb_buckets as u64)) as usize;

                        let hash_table_start = 2;
                        let bucket_location = storage.get(hash_table_start + bucket_index) as usize;
                        let bucket_content_start = bucket_location + 1;

                        // println!("key {}: Inserting in bucket {}/{}", key, bucket_index, nb_buckets);

                        // Range of addresses that need to be locked
                        // TODO StaticAddress should be u64 or usize?
                        // TODO Should specify whether its read or write -> signed integers?
                        // TODO Adapt scheduling
                        tx.addresses[0] = bucket_location as StaticAddress;
                        tx.addresses[1] = (bucket_content_start + bucket_capacity * PARAM_COUNT) as StaticAddress;

                        tx.function = AtomicFunction::PieceDHashMap(op.next_operation());
                        FunctionResult::Another(tx)
                    }
                    TryInsert => {

                        let bucket_location = tx.addresses[0] as usize;
                        let bucket_end = tx.addresses[1] as usize;
                        let key = tx.params[0];
                        // println!("------------ INSERT ---------------");
                        let bucket_size_mut = storage.get_mut(bucket_location as usize);
                        let bucket_content_start = bucket_location + 1;
                        match DHashMap::search_bucket::<PARAM_COUNT>(key, bucket_content_start, bucket_end, &mut storage) {
                            SearchResult::Entry(_entry, index) => {
                                let mut previous = [0; PARAM_COUNT];
                                for offset in 0..PARAM_COUNT {
                                    let current = storage.get_mut(index + offset);
                                    previous[offset] = *current;
                                    *current = tx.params[offset] as Word;
                                }

                                FunctionResult::DHashMap(Ok(Replaced(previous)))
                            },
                            SearchResult::EmptySpot(_entry, index) => {
                                *bucket_size_mut += 1;
                                for offset in 0..PARAM_COUNT {
                                    let current = storage.get_mut(index + offset);
                                    *current = tx.params[offset] as Word;
                                }

                                FunctionResult::DHashMap(Ok(Inserted))
                            },
                            SearchResult::BucketFull => {

                                // Trigger a full resize
                                tx.function = AtomicFunction::PieceDHashMap(ResizeInsert);
                                // TODO set Exclusive address (write all) and adapt scheduling
                                FunctionResult::Another(tx)
                            }
                        }
                    },
                    ResizeInsert => {
                        // println!("============= RESIZE =============");
                        let key_to_insert = tx.params[0];
                        let hash = d_hash_map::DHashMap::compute_hash(key_to_insert);

                        let nb_buckets = storage.get(0) as usize;
                        let bucket_capacity_elems = storage.get(1) as usize;
                        let bucket_capacity = 1 + bucket_capacity_elems * PARAM_COUNT;
                        let hash_table_start = 2;

                        // Check if bucket is still full (otherwise just insert)
                        let bucket_index = (hash % (nb_buckets as u64)) as usize;
                        let bucket_location = storage.get(hash_table_start + bucket_index) as usize;
                        let bucket_size = storage.get(bucket_location) as usize;

                        if bucket_size < bucket_capacity_elems {
                            let bucket_content_start = bucket_location + 1;
                            let bucket_end = bucket_content_start + bucket_capacity * PARAM_COUNT;

                            tx.addresses[0] = bucket_location as StaticAddress;
                            tx.addresses[1] = bucket_end as StaticAddress;

                            // Try inserting again
                            return AtomicFunction::PieceDHashMap(TryInsert).execute(tx, storage);
                        }

                        let old_hash_table_size = hash_table_start + nb_buckets;
                        let old_capacity = old_hash_table_size + nb_buckets * bucket_capacity;

                        // Double the number of buckets
                        let more_buckets = 2 * nb_buckets;
                        let new_hash_table_size = hash_table_start + more_buckets;
                        let new_capacity = new_hash_table_size + more_buckets * bucket_capacity;

                        // If the new size is too large for vm storage return error
                        if new_capacity > storage.len() {
                            // TODO return an error
                            panic!("Can't resize hash map, not enough space")
                        }

                        // Prepare the bigger DHashMap
                        let mut new_map = vec![0; new_capacity];
                        // TODO no need to add sentinels during init since many of them will be overwritten, add them after the buckets have been filled
                        DHashMap::init::<PARAM_COUNT>(&mut new_map, more_buckets, bucket_capacity_elems);

                        // Insert all entries in the new map:
                        // for each bucket of the old map
                        for old_bucket_index in 0..nb_buckets {
                            // TODO Make a function for that?
                            let old_bucket_location = storage.get(hash_table_start + old_bucket_index) as usize;
                            let old_bucket_size = storage.get(old_bucket_location) as usize;

                            let mut current_index = (old_bucket_location + 1) as usize;
                            let mut moved = 0;
                            while moved < old_bucket_size {
                                let old_entry_start = current_index;
                                let key = storage.get(old_entry_start);
                                current_index += PARAM_COUNT;

                                if key != DHashMap::SENTINEL {
                                    assert_ne!(key, DHashMap::LAST);    // We still have entries to move, how could we have reached the end?

                                    // TODO Store hash with the entry so that we don't need to compute it again?
                                    let hash = d_hash_map::DHashMap::compute_hash(key as FunctionParameter);
                                    let new_bucket_index = (hash % (more_buckets as u64)) as usize;

                                    let new_bucket_location = new_map[hash_table_start + new_bucket_index] as usize;
                                    let new_bucket_size = new_map[new_bucket_location] as usize;    // == next_entry_index
                                    if new_bucket_size >= bucket_capacity {
                                        panic!("A bucket is full during resize!!!");
                                    }

                                    // We can add it directly at the end of the new bucket
                                    let entry_start = (new_bucket_location + 1) + new_bucket_size * PARAM_COUNT;
                                    for offset in 0..PARAM_COUNT {
                                        new_map[entry_start + offset] = storage.get(old_entry_start + offset);
                                    }

                                    new_map[new_bucket_location] += 1;
                                    moved += 1;
                                }
                            }
                        }

                        // put the new map in place of the previous hashmap
                        storage.ptr.copy_from_nonoverlapping(new_map.as_ptr(), new_capacity);

                        // TODO Just for debug
                        // DHashMap::println_ptr::<PARAM_COUNT>(storage.ptr, storage.len());

                        // Actually insert the new value
                        let bucket_index = (hash % (more_buckets as u64)) as usize;

                        let bucket_location = storage.get(hash_table_start + bucket_index) as usize;
                        let bucket_content_start = bucket_location + 1;
                        let bucket_end = bucket_content_start + bucket_capacity * PARAM_COUNT;
                        tx.addresses[0] = bucket_location as StaticAddress;
                        tx.addresses[1] = bucket_end as StaticAddress;

                        // Try inserting again
                        AtomicFunction::PieceDHashMap(TryInsert).execute(tx, storage)
                    },
                    Get => {
                        let bucket_location = tx.addresses[0] as usize;
                        let bucket_end = tx.addresses[1] as usize;
                        let key = tx.params[0];
                        let bucket_content_start = bucket_location + 1;
                        // println!("------------ GET ---------------");
                        match DHashMap::search_bucket::<PARAM_COUNT>(key, bucket_content_start, bucket_end, &mut storage) {
                            SearchResult::Entry(_entry, index) => {
                                let mut found = [0; PARAM_COUNT];
                                for offset in 0..PARAM_COUNT {
                                    found[offset] = storage.get(index + offset);
                                }
                                FunctionResult::DHashMap(Ok(Value(found)))
                            },
                            _ => {
                                FunctionResult::DHashMap(Ok(None))
                            },
                        }
                    },
                    Remove => {
                        let bucket_location = tx.addresses[0] as usize;
                        let bucket_end = tx.addresses[1] as usize;
                        let key = tx.params[0];
                        let bucket_size_mut = storage.get_mut(bucket_location);
                        let bucket_content_start = bucket_location + 1;
                        // println!("------------ REMOVE ---------------");
                        match DHashMap::search_bucket::<PARAM_COUNT>(key, bucket_content_start, bucket_end, &mut storage) {
                            SearchResult::Entry(_entry, index) => {
                                *bucket_size_mut -= 1;
                                let mut found = [0; PARAM_COUNT];
                                for offset in 0..PARAM_COUNT {
                                    found[offset] = storage.get(index + offset);
                                }
                                // // Lazy version: only mark the current entry as "last" => search will take longer
                                storage.set(index, DHashMap::SENTINEL);

                                let next_index = index + PARAM_COUNT;
                                if next_index >= bucket_end || storage.get(next_index) == DHashMap::LAST {
                                    // Make sure to mark the end of the bucket to speed up future search operations
                                    let mut new_last_index = index;
                                    while storage.get(new_last_index) == DHashMap::SENTINEL {
                                        storage.set(new_last_index, DHashMap::LAST);
                                        if new_last_index > bucket_content_start {
                                            new_last_index -= PARAM_COUNT;
                                        } else {
                                            break;
                                        }
                                    }
                                }

                                FunctionResult::DHashMap(Ok(Value(found)))
                            },
                            _ => {
                                FunctionResult::DHashMap(Ok(None))
                            },
                        }
                    },
                    Has => {
                        let bucket_start = tx.addresses[0] as usize;
                        let bucket_end = tx.addresses[1] as usize;
                        let key = tx.params[0];
                        // println!("------------ HAS ---------------");
                        match DHashMap::search_bucket::<PARAM_COUNT>(key, bucket_start, bucket_end, &mut storage) {
                            SearchResult::Entry(_, _) => FunctionResult::DHashMap(Ok(HasKey(true))),
                            _ => FunctionResult::DHashMap(Ok(HasKey(false))),
                        }
                    },
                };

                res
            },
            _ => todo!()
        }
    }

    fn fib(n: FunctionParameter) -> FunctionParameter {
        if n <= 1 {
            1
        } else {
            AtomicFunction::fib(n-1) + AtomicFunction::fib(n-2)
        }
    }
}

#[derive(Debug)]
pub struct SharedMap<'a, V: Clone + Debug> {
    pub base_address: Cell<*mut Option<V>>,
    pub capacity: usize,
    pub size: usize,
    _lifetime: PhantomData<&'a ()>
}

// TODO Add mutex to be able to check if concurrent access is really prevented by scheduling?
impl<'a, V: Clone + Debug> SharedMap<'a, V> {

    #[inline]
    pub fn from_ptr(base_address: Cell<*mut Option<V>>, size: usize, capacity: usize) -> SharedMap<'a, V> {
        let required = size * mem::size_of::<Option<V>>();
        if capacity < required {
            panic!("Not enough capacity to fit {} elements ({} but only {} available)", size, required, capacity);
        }
        Self{
            base_address,
            capacity,
            size,
            _lifetime: PhantomData,
        }
    }

    pub fn new(base_address: Cell<*mut Option<V>>, size: usize, capacity: usize) -> SharedMap<'a, V> {
        let required = size * mem::size_of::<Option<V>>();
        if capacity < required {
            panic!("Not enough capacity to fit {} elements ({} but only {} available)", size, required, capacity);
        }
        unsafe {
            let mut v = Vec::from_raw_parts(base_address.get(), size, size);
            v.fill(None);
            mem::forget(v);
        }

        Self{
            base_address,
            capacity,
            size,
            _lifetime: PhantomData,
        }
    }

    pub fn new_with_default(base_address: Cell<*mut Option<V>>, size: usize, capacity: usize, default_value: V) -> SharedMap<'a, V> {
        let required = size * mem::size_of::<Option<V>>();
        if capacity < required {
            panic!("Not enough capacity to fit {} elements (o{} but nly {} available)", size, required, capacity);
        }
        unsafe {
            let mut v = Vec::from_raw_parts(base_address.get(), size, size);
            v.fill(Some(default_value.clone()));
            mem::forget(v);
        }

        Self{
            base_address,
            capacity,
            size,
            _lifetime: PhantomData,
        }
    }

    #[inline]
    pub unsafe fn get(&self, key: StaticAddress) -> Option<&'a V> {
        if key as usize >= self.size { panic!("Access out of range: key {} >= {}", key, self.size); }

        let value = self.base_address.get().add(key as usize);
        (*value).as_ref()
    }

    #[inline]
    pub unsafe fn get_mut(&self, key: StaticAddress) -> Option<&'a mut V> {
        if key as usize >= self.size { panic!("Access out of range: key {} >= {}", key, self.size); }

        let value = self.base_address.get().add(key as usize);
        (*value).as_mut()
    }

    #[inline]
    pub unsafe fn insert(&self, key: StaticAddress, new_value: V) -> Option<V> {
        if key as usize >= self.size { panic!("Access out of range: key {} >= {}", key, self.size); }

        let value = self.base_address.get().add(key as usize);
        (*value).replace(new_value)
    }
}

#[derive(Debug, Clone)]
pub struct TestSharedMap {
    pub address: StaticAddress,
    pub weight: u64,
    pub delegate: Option<StaticAddress>,
    pub vote: Option<usize>,
    pub test_bool: bool,
    pub test_u16: u16,
}

impl TestSharedMap {

    fn new(address: StaticAddress, weight: u64) -> Self {
        Self {
            address,
            weight,
            delegate: None,
            vote: None,
            test_bool: false,
            test_u16: 0,
        }
    }

    pub fn test_new() {
        println!("Testing SharedMap::new");
        let nb_elements = 4;
        let mut test: Vec<Option<TestSharedMap>> = Vec::with_capacity(nb_elements);
        let value_size = mem::size_of::<Option<TestSharedMap>>();
        let capacity = nb_elements * value_size;

        unsafe {
            println!("Testing new map:");
            let shared_map = SharedMap::new(Cell::new(test.as_mut_ptr()), nb_elements, capacity);
            let reference = shared_map.get(0);
            println!("get(0): {:?}", reference);
            let reference = shared_map.get(1);
            println!("get(1): {:?}", reference);
            let reference = shared_map.get(2);
            println!("get(2): {:?}", reference);
            let reference = shared_map.get(3);
            println!("get(3): {:?}", reference);
            // Index 4 should panic
            // let reference = shared_map.get::<TestSharedMap>(4);
            // println!("get(4): {:?}", reference);
            println!();

            let default_value = TestSharedMap::new(42, 20);
            println!("Testing new map with default value: {:?}", default_value);

            let shared_map = SharedMap::new_with_default(Cell::new(test.as_mut_ptr()), nb_elements, capacity, default_value.clone());
            let reference = shared_map.get(0);
            println!("get(0): {:?}", reference);
            let reference = shared_map.get(1);
            println!("get(1): {:?}", reference);
            let reference = shared_map.get(2);
            println!("get(2): {:?}", reference);
            let reference = shared_map.get(3);
            println!("get(3): {:?}", reference);
            // Index 4 should panic
            // let reference = shared_map.get::<TestSharedMap>(4);
            // println!("get(4): {:?}", reference);
            println!();

            println!("Testing new map + inserting: {:?}", default_value);
            let shared_map = SharedMap::new_with_default(Cell::new(test.as_mut_ptr()), nb_elements, capacity, default_value.clone());
            shared_map.insert(0, default_value.clone());
            shared_map.insert(1, default_value.clone());
            shared_map.insert(2, default_value.clone());
            shared_map.insert(3, default_value.clone());
            let reference = shared_map.get(0);
            println!("get(0): {:?}", reference);
            let reference = shared_map.get(1);
            println!("get(1): {:?}", reference);
            let reference = shared_map.get(2);
            println!("get(2): {:?}", reference);
            let reference = shared_map.get(3);
            println!("get(3): {:?}", reference);
            // Index 4 should panic
            // let reference = shared_map.get::<TestSharedMap>(4);
            // println!("get(4): {:?}", reference);

            println!();
        }
    }

    pub fn test_many_inserts() {
        println!("Testing SharedMap::insert (many times)");
        let nb_elements = 4;
        let capacity = nb_elements * mem::size_of::<Option<TestSharedMap>>();
        let mut test = Vec::with_capacity(nb_elements);
        let mut shared_map = SharedMap::new(Cell::new(test.as_mut_ptr()), nb_elements, capacity);

        let try_get = |shared: &mut SharedMap<TestSharedMap>, key: StaticAddress| unsafe {
            println!("map.get({}):", key);
            if let Some(voter) = shared.get(key) {
                println!("\t{:?}", voter);
            } else {
                println!("\tNone");
            }
        };

        let insert_new_voter = |shared: &mut SharedMap<TestSharedMap>, key: StaticAddress, weight: u64, delegate: Option<StaticAddress>| unsafe {
            let mut new_entry = TestSharedMap::new(key, weight);
            new_entry.delegate = delegate;
            println!("Inserting new voter with key {}:\n{:?}", key, new_entry);
            shared.insert(key, new_entry);
        };

        unsafe {
            println!("Map is initially empty:");
            try_get(&mut shared_map, 0);
            try_get(&mut shared_map, 1);
            try_get(&mut shared_map, 2);
            try_get(&mut shared_map, 3);
            // try_get(&mut shared_map, 4); // Should panic
            println!();

            insert_new_voter(&mut shared_map, 0, 0, None);
            insert_new_voter(&mut shared_map, 1, 10, None);
            insert_new_voter(&mut shared_map, 2, 20, None);
            insert_new_voter(&mut shared_map, 3, 30, Some(1));
            println!();

            try_get(&mut shared_map, 0);
            try_get(&mut shared_map, 1);
            try_get(&mut shared_map, 2);
            try_get(&mut shared_map, 3);
            // try_get(&mut shared_map, 4); // Should panic
            println!();

            if let Some(voter) = shared_map.get_mut(0) {
                println!("Modifying entry 0: test_bool <- true");
                voter.test_bool = true;
            }

            if let Some(voter) = shared_map.get_mut(1) {
                println!("Modifying entry 1: vote <- Some(42)");
                voter.vote = Some(42);
            }

            if let Some(voter) = shared_map.get_mut(2) {
                println!("Modifying entry 2: test_u16 <- 16");
                voter.test_u16 = 16;
            }
            println!();

            try_get(&mut shared_map, 0);
            try_get(&mut shared_map, 1);
            try_get(&mut shared_map, 2);
            try_get(&mut shared_map, 3);
            // try_get(&mut shared_map, 4); // Should panic
            println!();
        }
    }

    pub fn test_single_insert_struct() {
        println!("Testing single insert of struct");
        let max_nb_entries = 4;
        let capacity = max_nb_entries * mem::size_of::<Option<TestSharedMap>>();

        let extra = 10;
        let mut test = Vec::with_capacity(max_nb_entries + extra);
        let mut shared_map = SharedMap::new(Cell::new(test.as_mut_ptr()), max_nb_entries, capacity);

        unsafe {
            println!("Map is initially empty:");
            let reference = shared_map.get(0);
            println!("map.get(0): {:?}", reference);

            let mut new_entry = TestSharedMap::new(20, 1);
            new_entry.vote = Some(42);
            println!("Inserting new entry with key 0:\n\t {:?}", new_entry);
            shared_map.insert(0, new_entry);

            let reference = shared_map.get(0);
            println!("map.get(0): {:?}", reference);
            let reference = shared_map.get(0);
            println!("map.get(0): {:?}", reference);
            println!();
        }
    }

    pub fn test_single_insert_balance() {
        println!("Testing insert of primitive integer (u16)");
        type Currency = u16;
        let max_nb_entries = 4;

        let capacity = max_nb_entries * mem::size_of::<Option<Currency>>();
        let mut test: Vec<Option<Currency>> = Vec::with_capacity(max_nb_entries);
        let mut shared_map = SharedMap::new(Cell::new(test.as_mut_ptr()), max_nb_entries, capacity);

        unsafe {
            println!("Map is initially empty:");
            let reference = shared_map.get(0);
            println!("map.get(0): {:?}", reference);
            assert_eq!(reference, None);
            println!();

            let mut new_entry: Currency = (200 as Currency).into();
            println!("Inserting new entry with key 0:\t {:?}", new_entry);
            let old_value = shared_map.insert(0, new_entry);
            assert_eq!(old_value, None);

            let reference = shared_map.get(0);
            println!("map.get(0): {:?}", reference);
            assert_eq!(reference, Some(&new_entry));
            println!();

            println!("Multiplying entry 0 by 2");
            let mut balance = shared_map.get_mut(0).unwrap();
            let previous_balance = *balance;
            let new_balance = 2 * previous_balance;
            *balance = new_balance;

            let reference = shared_map.get(0);
            println!("map.get(0): {:?}", reference);
            assert_eq!(reference, Some(&new_balance));
            println!()
        }

        println!("Testing insert of primitive integer (u128)");
        type BigCurrency = u128;
        let max_nb_entries = 4;

        let capacity = max_nb_entries * mem::size_of::<Option<BigCurrency>>();
        let mut test: Vec<Option<BigCurrency>> = Vec::with_capacity(max_nb_entries);
        let mut shared_map = SharedMap::new(Cell::new(test.as_mut_ptr()), max_nb_entries, capacity);

        unsafe {
            println!("Map is initially empty:");
            let reference = shared_map.get(0);
            println!("map.get(0): {:?}", reference);
            assert_eq!(reference, None);
            println!();

            let mut new_entry: BigCurrency = (200 as BigCurrency).into();
            println!("Inserting new entry with key 0:\t {:?}", new_entry);
            let old_value = shared_map.insert(0, new_entry);
            assert_eq!(old_value, None);

            let reference = shared_map.get(0);
            println!("map.get(0): {:?}", reference);
            assert_eq!(reference, Some(&new_entry));
            println!();

            println!("Multiplying entry 0 by 2");
            let mut balance = shared_map.get_mut(0).unwrap();
            let previous_balance = *balance;
            let new_balance = 2 * previous_balance;
            *balance = new_balance;

            let reference = shared_map.get(0);
            println!("map.get(0): {:?}", reference);
            assert_eq!(reference, Some(&new_balance));
        }
    }

    pub fn test_single_insert_base_u8() {
        let max_nb_entries = 4;
        let capacity = max_nb_entries * mem::size_of::<Option<Balance>>();
        let mut test: Vec<u8> = Vec::with_capacity(capacity);

        let mut shared_map = SharedMap::new(Cell::new(test.as_mut_ptr() as *mut Option<Balance>), max_nb_entries, capacity);

        unsafe {
            println!("Map is initially empty:");
            let reference = shared_map.get(0);
            println!("map.get(0): {:?}", reference);

            let mut new_entry = Balance{ amount: 42 };
            println!("Inserting new entry with key 0:\n\t {:?}", new_entry);
            shared_map.insert(0, new_entry);

            let reference = shared_map.get(0);
            println!("map.get(0):\n\t{:?}", reference);
        }

        println!();
        let max_nb_entries = 4;
        let capacity = max_nb_entries * mem::size_of::<Option<TestSharedMap>>();
        let mut test: Vec<u8> = Vec::with_capacity(capacity);

        let mut shared_map = SharedMap::new(Cell::new(test.as_mut_ptr() as *mut Option<TestSharedMap>), max_nb_entries, capacity);

        unsafe {
            println!("Map is initially empty:");
            let reference = shared_map.get(0);
            println!("map.get(0): {:?}", reference);

            let mut new_entry = TestSharedMap::new(20, 1);
            new_entry.vote = Some(42);
            println!("Inserting new entry with key 0:\n\t {:?}", new_entry);
            shared_map.insert(0, new_entry);

            let reference = shared_map.get(0);
            println!("map.get(0):\n\t{:?}", reference);
        }
    }

    pub fn test_all() {
        // TODO Add asserts to all tests
        Self::test_single_insert_balance();
        Self::test_single_insert_base_u8();
        Self::test_single_insert_struct();
        Self::test_new();
        Self::test_many_inserts();
    }
}

#[repr(C)]
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
struct Balance {
    pub amount: u64
}

