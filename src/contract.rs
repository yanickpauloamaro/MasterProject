use std::cell::Cell;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::mem;
use std::ops::Range;
use rayon::range::Iter;

use crate::{auction, d_hash_map, key_value};
use crate::auction::SimpleAuction;
use crate::d_hash_map::{DHash, DHashMap};
use crate::key_value::{KeyValue, KeyValueOperation, Value};
use crate::vm_utils::SharedStorage;
use crate::wip::Word;

pub type SenderAddress = u32;
pub type FunctionAddress = u32;
pub type StaticAddress = u32;
pub type FunctionParameter = u32;

pub const MAX_NB_ADDRESSES: usize = 2;
pub const MAX_NB_PARAMETERS: usize = 2;

#[derive(Eq, PartialEq, Copy, Clone)]
pub enum AccessType {
    Read,
    Write,
    TryWrite
}

impl AccessType {
    #[inline]
    pub fn is_read(&self) -> bool {
        *self == AccessType::Read
    }
}

#[derive(Eq, PartialEq, Clone, Copy, Debug)]
pub enum AccessPattern {
    Address(StaticAddress),
    Range(StaticAddress, StaticAddress),
    All,
    Done
}

impl AccessPattern {
    pub fn addresses(&self) -> Range<StaticAddress> {
        match self {
            AccessPattern::Address(addr) => {
                *addr..(*addr+1)
            },
            AccessPattern::Range(from, to) => {
                *from..*to
            },
            other => panic!("Access patter {:?} can't be iterated on", other)
        }
    }
}

#[derive(Eq, PartialEq, Clone, Copy)]
pub enum Access {
    Address(StaticAddress, AccessType),
    Range(StaticAddress, StaticAddress, AccessType),
    All(AccessType),
    Done
}

#[derive(Clone, Debug, Copy)]
pub struct Transaction<const ADDRESS_COUNT: usize, const PARAM_COUNT: usize> {
    pub sender: SenderAddress,
    pub function: AtomicFunction,
    pub tx_index: usize,    // Could use u32
    pub addresses: [StaticAddress; ADDRESS_COUNT],
    pub params: [FunctionParameter; PARAM_COUNT],
}

pub enum TransactionType {
    ReadOnly,
    Exclusive,
    Writes
}
impl<const ADDRESS_COUNT: usize, const PARAM_COUNT: usize> Transaction<ADDRESS_COUNT, PARAM_COUNT> {
    // TODO Add AccessType inside AccessPattern so that only 1 array need to be returned
    pub fn mixed_accesses(&self) -> Option<[Access; ADDRESS_COUNT]> {
        let mut accesses = [Access::Done; ADDRESS_COUNT];

        match self.function {
            AtomicFunction::Transfer => {
                accesses[0] = Access::Address(self.addresses[0], AccessType::Write);
                accesses[1] = Access::Address(self.addresses[1], AccessType::Write);
                Some(accesses)
            },
            AtomicFunction::TransferDecrement | AtomicFunction::TransferIncrement => {
                accesses[0] = Access::Address(self.addresses[0], AccessType::Write);
                Some(accesses)
            },
            AtomicFunction::Fibonacci => None,
            AtomicFunction::DHashMap(_) => {
                accesses[0] = Access::All(AccessType::Write);
                Some(accesses)
            },
            AtomicFunction::PieceDHashMap(op) => {
                use d_hash_map::PiecedOperation::*;
                match op {
                    InsertComputeHash | RemoveComputeHash | GetComputeHash | HasComputeHash => None,
                    InsertFindBucket | RemoveFindBucket | GetFindBucket | HasFindBucket => {
                        // let hash_table_start = 0;
                        // reads[0] = AccessPattern::Address(hash_table_start);
                        // (Some(reads), None)

                        let hash_table_start = 0;
                        accesses[0] = Access::Address(hash_table_start, AccessType::Read);
                        Some(accesses)
                    },
                    InsertComputeAndFind | RemoveComputeAndFind | GetComputeAndFind | HasComputeAndFind => {
                        // let hash_table_start = 0;
                        // reads[0] = AccessPattern::Address(hash_table_start);
                        // (Some(reads), None)

                        let hash_table_start = 0;
                        accesses[0] = Access::Address(hash_table_start, AccessType::Read);
                        Some(accesses)
                    },
                    Insert | Remove => {
                        // let hash_table_start = 0;
                        // let bucket_location = self.addresses[0] as StaticAddress;
                        //
                        // reads[0] = AccessPattern::Address(hash_table_start);
                        // writes[0] = AccessPattern::Address(bucket_location);
                        // (Some(reads), Some(writes))

                        let hash_table_start = 0;
                        let bucket_location = self.addresses[0] as StaticAddress;
                        accesses[0] = Access::Address(hash_table_start, AccessType::Read);
                        accesses[1] = Access::Address(bucket_location, AccessType::Write);
                        Some(accesses)
                    },
                    Resize => {
                        // writes[0] = AccessPattern::All;
                        // (None, Some(writes))
                        accesses[0] = Access::All(AccessType::Write);
                        Some(accesses)
                    },
                    Get | Has => {
                        // let hash_table_start = 0;
                        // let bucket_location = self.addresses[0] as StaticAddress;
                        //
                        // reads[0] = AccessPattern::Address(hash_table_start);
                        // reads[1] = AccessPattern::Address(bucket_location);
                        // (Some(reads), None)

                        let hash_table_start = 0;
                        let bucket_location = self.addresses[0] as StaticAddress;
                        accesses[0] = Access::Address(hash_table_start, AccessType::Read);
                        accesses[1] = Access::Address(bucket_location, AccessType::Read);
                        Some(accesses)
                    }
                }
            },
            _ => todo!()
        }
    }

    pub fn tpe(&self) -> TransactionType {

        match self.function {
            AtomicFunction::Transfer => TransactionType::Writes,
            AtomicFunction::TransferDecrement | AtomicFunction::TransferIncrement => TransactionType::Writes,
            AtomicFunction::Fibonacci => TransactionType::ReadOnly,
            AtomicFunction::DHashMap(_) => TransactionType::Exclusive,
            AtomicFunction::PieceDHashMap(op) => {
                use d_hash_map::PiecedOperation::*;
                match op {
                    InsertComputeHash | RemoveComputeHash | GetComputeHash | HasComputeHash => TransactionType::ReadOnly,
                    InsertFindBucket | RemoveFindBucket | GetFindBucket | HasFindBucket => TransactionType::ReadOnly,
                    InsertComputeAndFind | RemoveComputeAndFind | GetComputeAndFind | HasComputeAndFind => TransactionType::ReadOnly,
                    Insert | Remove => TransactionType::Writes,
                    Resize => TransactionType::Exclusive,
                    Get | Has => TransactionType::ReadOnly
                }
            },
            _ => todo!()
        }
    }

    pub fn accesses(&self) -> (Option<[AccessPattern; ADDRESS_COUNT]>, Option<[AccessPattern; ADDRESS_COUNT]>) {
        let mut reads = [AccessPattern::Done; ADDRESS_COUNT];
        let mut writes = [AccessPattern::Done; ADDRESS_COUNT];

        match self.function {
            AtomicFunction::Transfer => {
                writes[0] = AccessPattern::Address(self.addresses[0]);
                writes[1] = AccessPattern::Address(self.addresses[1]);
                (None, Some(writes))
            },
            AtomicFunction::TransferDecrement | AtomicFunction::TransferIncrement => {
                writes[0] = AccessPattern::Address(self.addresses[0]);
                (None, Some(writes))
            },
            AtomicFunction::Fibonacci => (None, None),
            AtomicFunction::DHashMap(_) => {
                writes[0] = AccessPattern::All;
                (None, Some(writes))
            },
            AtomicFunction::PieceDHashMap(op) => {
                use d_hash_map::PiecedOperation::*;
                match op {
                    InsertComputeHash | RemoveComputeHash | GetComputeHash | HasComputeHash => (None, None),
                    InsertFindBucket | RemoveFindBucket | GetFindBucket | HasFindBucket => {
                        let hash_table_start = 0;
                        reads[0] = AccessPattern::Address(hash_table_start);
                        (Some(reads), None)
                    },
                    InsertComputeAndFind | RemoveComputeAndFind | GetComputeAndFind | HasComputeAndFind => {
                        let hash_table_start = 0;
                        reads[0] = AccessPattern::Address(hash_table_start);
                        (Some(reads), None)
                    },
                    Insert | Remove => {
                        let hash_table_start = 0;
                        let bucket_location = self.addresses[0] as StaticAddress;

                        reads[0] = AccessPattern::Address(hash_table_start);
                        writes[0] = AccessPattern::Address(bucket_location);
                        (Some(reads), Some(writes))
                    },
                    Resize => {
                        writes[0] = AccessPattern::All;
                        (None, Some(writes))
                    },
                    Get | Has => {
                        let hash_table_start = 0;
                        let bucket_location = self.addresses[0] as StaticAddress;

                        reads[0] = AccessPattern::Address(hash_table_start);
                        reads[1] = AccessPattern::Address(bucket_location);
                        (Some(reads), None)
                    }
                }
            },
            _ => todo!()
        }
    }
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

                // Equivalent of *Request for pieced version
                let key = tx.params[0];
                let (
                    hash,
                    _bucket_index,
                    bucket_location,
                    bucket_end
                ) = DHashMap::get_bucket::<PARAM_COUNT>(key, &storage);

                use d_hash_map::Operation::*;
                match op {
                    Get => {
                        let res = DHashMap::get_entry_from_bucket::<PARAM_COUNT>(key, bucket_location, bucket_end, &storage);
                        FunctionResult::DHashMap(res)
                    },
                    Remove => {
                        let res = DHashMap::remove_entry_from_bucket::<PARAM_COUNT>(key, bucket_location, bucket_end, &mut storage);
                        FunctionResult::DHashMap(res)
                    },
                    ContainsKey => {
                        let res = DHashMap::check_key_in_bucket::<PARAM_COUNT>(key, bucket_location, bucket_end, &storage);
                        FunctionResult::DHashMap(res)
                    }
                    Insert => {
                        // TODO Change FunctionParameter to u64?
                        let new_value: [Word; PARAM_COUNT] = tx.params.map(|el| el as Word);

                        let res = DHashMap::insert_entry_in_bucket::<PARAM_COUNT>(key, bucket_location, bucket_end, &new_value, &mut storage);
                        match res {
                            Ok(success) => FunctionResult::DHashMap(Ok(success)),
                            Err(d_hash_map::Error::BucketFull) => {
                                let resize_and_retry = DHashMap::resize_and_insert::<PARAM_COUNT>(key, hash, &new_value, &mut storage);
                                FunctionResult::DHashMap(resize_and_retry)
                            },
                            Err(_) => FunctionResult::DHashMap(Err(d_hash_map::Error::IllegalState))
                        }
                    },
                }
            },
            AtomicFunction::PieceDHashMap(op) => {

                use d_hash_map::PiecedOperation::*;
                use d_hash_map::Success::*;

                let res = match op {
                    InsertComputeHash | RemoveComputeHash | GetComputeHash | HasComputeHash => {
                        // println!("------------ Compute Hash ---------------");

                        let key = tx.params[0];

                        let hash = DHashMap::compute_hash(key);
                        let (hash_low, hash_high) = DHashMap::hash_to_halves(hash);

                        tx.addresses[0] = StaticAddress::MAX;
                        tx.addresses[1] = StaticAddress::MAX;
                        tx.addresses[2] = hash_low as StaticAddress;
                        tx.addresses[3] = hash_high as StaticAddress;
                        tx.addresses[4] = StaticAddress::MAX;

                        tx.function = AtomicFunction::PieceDHashMap(op.next_operation());
                        FunctionResult::Another(tx)
                    },
                    InsertFindBucket | RemoveFindBucket | GetFindBucket | HasFindBucket => {
                        // println!("------------ FIND BUCKET ---------------");
                        let hash_low = tx.addresses[2];
                        let hash_high = tx.addresses[3];

                        let current_nb_buckets = storage.get(0);
                        // TODO replace change static address into a u64 so I can store a DHash entirely?
                        let hash = DHashMap::hash_from_halves(hash_low as DHash, hash_high as DHash);
                        let (
                            _hash,
                            _bucket_index,
                            bucket_location,
                            bucket_end
                        ) = DHashMap::get_bucket_from_hash::<PARAM_COUNT>(hash , &storage);

                        tx.addresses[0] = bucket_location as StaticAddress;
                        tx.addresses[1] = bucket_end as StaticAddress;
                        tx.addresses[4] = current_nb_buckets as StaticAddress;

                        tx.function = AtomicFunction::PieceDHashMap(op.next_operation());
                        FunctionResult::Another(tx)
                    },
                    InsertComputeAndFind | RemoveComputeAndFind | GetComputeAndFind | HasComputeAndFind => {
                        // println!("------------ Compute Hash and Find Bucket ---------------");
                        let key = tx.params[0];
                        let (
                            hash,
                            _bucket_index,
                            bucket_location,
                            bucket_end
                        ) = DHashMap::get_bucket::<PARAM_COUNT>(key, &storage);

                        let current_nb_buckets = storage.get(0);
                        let (hash_low, hash_high) = DHashMap::hash_to_halves(hash);

                        tx.addresses[0] = bucket_location as StaticAddress;
                        tx.addresses[1] = bucket_end as StaticAddress;
                        tx.addresses[2] = hash_low as StaticAddress;
                        tx.addresses[3] = hash_high as StaticAddress;
                        tx.addresses[4] = current_nb_buckets as StaticAddress;

                        tx.function = AtomicFunction::PieceDHashMap(op.next_operation());
                        FunctionResult::Another(tx)
                    },
                    Insert => {

                        if DHashMap::is_outdated(&tx, &storage) {
                            // A resize happened since this piece was generated, the bucket location need to be updated
                            let updated_tx = DHashMap::update_tx(tx, &storage);
                            FunctionResult::Another(updated_tx)
                        } else {
                            // Bucket is still correct
                            let bucket_location = tx.addresses[0] as usize;
                            let bucket_end = tx.addresses[1] as usize;

                            let key = tx.params[0];
                            let new_value: [Word; PARAM_COUNT] = tx.params.map(|el| el as Word);

                            let res = DHashMap::insert_entry_in_bucket::<PARAM_COUNT>(key, bucket_location, bucket_end, &new_value, &mut storage);
                            match res {
                                Ok(success) => FunctionResult::DHashMap(Ok(success)),
                                Err(d_hash_map::Error::BucketFull) => {
                                    // println!("\t failed to insert entry {:?} in bucket {}", new_value, bucket_location);
                                    tx.addresses[0] = StaticAddress::MAX;
                                    tx.addresses[1] = StaticAddress::MAX;
                                    tx.function = AtomicFunction::PieceDHashMap(Resize);

                                    FunctionResult::Another(tx)
                                },
                                Err(_) => FunctionResult::DHashMap(Err(d_hash_map::Error::IllegalState))
                            }
                        }
                    },
                    Resize => {

                        let hash_low = tx.addresses[2] as DHash;
                        let hash_high = tx.addresses[3] as DHash;
                        let hash = DHashMap::hash_from_halves(hash_low as DHash, hash_high as DHash);

                        let key = tx.params[0];
                        // TODO Change FunctionParameter to u64 to avoid having to map?
                        let new_value: [Word; PARAM_COUNT] = tx.params.map(|el| el as Word);

                        let always_retry = false;

                        if always_retry {
                            let (
                                _hash,
                                _bucket_index,
                                bucket_location,
                                bucket_end
                            ) = DHashMap::get_bucket_from_hash::<PARAM_COUNT>(hash, &storage);

                            // Try to insert again, maybe a concurrent operation changed the hashmap
                            let retried = DHashMap::insert_entry_in_bucket::<PARAM_COUNT>(key, bucket_location, bucket_end, &new_value, &mut storage);
                            if retried.is_ok() {
                                return FunctionResult::DHashMap(retried);
                            }

                            let resize_and_retry = DHashMap::resize_and_insert::<PARAM_COUNT>(key, hash, &new_value, &mut storage);
                            FunctionResult::DHashMap(resize_and_retry)
                        } else {
                            if DHashMap::is_outdated(&tx, &storage) {
                                // A resize happened since this piece was generated, retry to insert
                                let (
                                    _hash,
                                    _bucket_index,
                                    bucket_location,
                                    bucket_end
                                ) = DHashMap::get_bucket_from_hash::<PARAM_COUNT>(hash, &storage);

                                let retried = DHashMap::insert_entry_in_bucket::<PARAM_COUNT>(key, bucket_location, bucket_end, &new_value, &mut storage);
                                if retried.is_ok() {
                                    // Successfully inserted, no need to resize
                                    return FunctionResult::DHashMap(retried);
                                }
                            }

                            // Bucket is still the same size or we failed to insert even though another resize happened
                            let resize_and_retry = DHashMap::resize_and_insert::<PARAM_COUNT>(key, hash, &new_value, &mut storage);
                            FunctionResult::DHashMap(resize_and_retry)
                        }
                    },
                    Get => {

                        if DHashMap::is_outdated(&tx, &storage) {
                            // A resize happened since this piece was generated, the bucket location need to be updated
                            let updated_tx = DHashMap::update_tx(tx, &storage);
                            FunctionResult::Another(updated_tx)
                        } else {
                            // Bucket is still correct
                            let bucket_location = tx.addresses[0] as usize;
                            let bucket_end = tx.addresses[1] as usize;

                            let key = tx.params[0];

                            let res = DHashMap::get_entry_from_bucket::<PARAM_COUNT>(key, bucket_location, bucket_end, &storage);
                            FunctionResult::DHashMap(res)
                        }
                    },
                    Remove => {
                        if DHashMap::is_outdated(&tx, &storage) {
                            // A resize happened since this piece was generated, the bucket location need to be updated
                            let updated_tx = DHashMap::update_tx(tx, &storage);
                            FunctionResult::Another(updated_tx)
                        } else {
                            // Bucket is still correct
                            let bucket_location = tx.addresses[0] as usize;
                            let bucket_end = tx.addresses[1] as usize;

                            let key = tx.params[0];

                            let res = DHashMap::remove_entry_from_bucket::<PARAM_COUNT>(key, bucket_location, bucket_end, &mut storage);
                            FunctionResult::DHashMap(res)
                        }
                    },
                    Has => {
                        if DHashMap::is_outdated(&tx, &storage) {
                            // A resize happened since this piece was generated, the bucket location need to be updated
                            let updated_tx = DHashMap::update_tx(tx, &storage);
                            FunctionResult::Another(updated_tx)
                        } else {
                            // Bucket is still correct
                            let bucket_location = tx.addresses[0] as usize;
                            let bucket_end = tx.addresses[1] as usize;

                            let key = tx.params[0];

                            let res = DHashMap::check_key_in_bucket::<PARAM_COUNT>(key, bucket_location, bucket_end, &storage);
                            FunctionResult::DHashMap(res)
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

//region SharedMap
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
//endregion