use std::{mem, slice};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use crate::d_hash_map::PiecedOperation::TryInsert;
use crate::vm_utils::SharedStorage;
use crate::wip::Word;

pub type DKey = u32;
pub type DHash = u64;

pub struct DHashMap;

impl DHashMap {
    #[inline]
    pub fn compute_hash(key: DKey) -> DHash {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        hasher.finish()
    }

    const HASH_TABLE_OFFSET: usize = 2;
    const BUCKET_CONTENT_OFFSET: usize = 1;
    pub const LAST: Word = Word::MAX;
    pub const SENTINEL: Word = Self::LAST - 1;

    #[inline]
    pub fn get_nb_buckets(storage: &SharedStorage) -> usize {
        storage.get(0) as usize
    }

    #[inline]
    pub fn get_bucket_capacity_elems(storage: &SharedStorage) -> usize {
        storage.get(1) as usize
    }

    pub fn get_bucket<const ENTRY_SIZE: usize>(key: DKey, storage: &SharedStorage) -> (DHash, usize, usize, usize) {
        let hash = Self::compute_hash(key);
        Self::get_bucket_from_hash::<ENTRY_SIZE>(hash, storage)
    }

    pub fn get_bucket_from_hash<const ENTRY_SIZE: usize>(hash: DHash, storage: &SharedStorage) -> (DHash, usize, usize, usize) {

        let nb_buckets = Self::get_nb_buckets(storage);
        let bucket_capacity_elems = Self::get_bucket_capacity_elems(storage);
        let bucket_index = (hash % (nb_buckets as u64)) as usize;

        let bucket_location = storage.get(Self::HASH_TABLE_OFFSET + bucket_index) as usize;
        let bucket_end = bucket_location + 1 + bucket_capacity_elems * ENTRY_SIZE;

        (hash, bucket_index, bucket_location, bucket_end)
    }

    //region Get
    pub fn get_entry_from_bucket<const ENTRY_SIZE: usize>(
        key: DKey,
        bucket_location: usize,
        bucket_end: usize,
        storage: &SharedStorage
    ) -> Result<ENTRY_SIZE>
    {
        // println!("------------ GET ---------------");
        match Self::search::<ENTRY_SIZE>(key, bucket_location, bucket_end, storage) {
            SearchResult::Entry(index) => {
                let mut found = [0; ENTRY_SIZE];
                for offset in 0..ENTRY_SIZE {
                    found[offset] = storage.get(index + offset);
                }
                Ok(Success::Value(found))
            },
            _ => Ok(Success::None),
        }
    }
    //endregion

    //region Has/ContainsKey
    pub fn check_key_in_bucket<const ENTRY_SIZE: usize>(
        key: DKey,
        bucket_location: usize,
        bucket_end: usize,
        storage: &SharedStorage
    ) -> Result<ENTRY_SIZE> {
        // println!("------------ HAS/CONTAINS_kEY ---------------");
        match Self::search::<ENTRY_SIZE>(key, bucket_location, bucket_end, storage) {
            SearchResult::Entry(_) => Ok(Success::HasKey(true)),
            _ => Ok(Success::HasKey(false)),
        }
    }
    //endregion

    //region Insert
    pub fn insert_entry_in_bucket<const ENTRY_SIZE: usize>(
        key: DKey,
        bucket_location: usize,
        bucket_end: usize,
        new_value: &[Word; ENTRY_SIZE],
        storage: &mut SharedStorage
    ) -> Result<ENTRY_SIZE>
    {
        // println!("------------ INSERT ---------------");
        match Self::search::<ENTRY_SIZE>(key, bucket_location, bucket_end, storage) {
            SearchResult::Entry(index) => {
                unsafe {
                    let mut previous = [0; ENTRY_SIZE];
                    let current = slice::from_raw_parts_mut(storage.ptr.add(index), ENTRY_SIZE);

                    // Copy the previous value
                    previous.copy_from_slice(current);

                    // Write the new value
                    current.copy_from_slice(new_value);

                    Ok(Success::Replaced(previous))
                }
            },
            SearchResult::EmptySpot(index) => {
                unsafe {
                    let current = slice::from_raw_parts_mut(storage.ptr.add(index), ENTRY_SIZE);

                    // Increment the size of the bucket
                    *storage.get_mut(bucket_location) += 1;

                    // Write the new value
                    current.copy_from_slice(new_value);

                    Ok(Success::Inserted)
                }
            },
            SearchResult::BucketFull => Err(Error::BucketFull)
        }
    }
    //endregion

    //region Remove
    pub fn remove_entry_from_bucket<const ENTRY_SIZE: usize>(
        key: DKey,
        bucket_location: usize,
        bucket_end: usize,
        storage: &mut SharedStorage
    ) -> Result<ENTRY_SIZE>
    {
        // println!("------------ REMOVE ---------------");
        match Self::search::<ENTRY_SIZE>(key, bucket_location, bucket_end, storage) {
            SearchResult::Entry(index) => {

                // Decrement the size of the bucket
                unsafe {
                    *storage.get_mut(bucket_location) -= 1;
                }

                // Copy the removed value
                let mut removed = [0; ENTRY_SIZE];
                for offset in 0..ENTRY_SIZE {
                    removed[offset] = storage.get(index + offset);
                }

                // Book keeping
                // Lazy version: only mark the current entry as "last" => future searches will take longer
                storage.set(index, DHashMap::SENTINEL);

                // Check if this is the last entry in the bucket
                let next_index = index + ENTRY_SIZE;
                if next_index >= bucket_end || storage.get(next_index) == DHashMap::LAST {
                    // Make sure to propagate the "end of bucket" sentinel to the previous entries
                    let mut new_last_index = index;
                    while storage.get(new_last_index) == DHashMap::SENTINEL {
                        storage.set(new_last_index, DHashMap::LAST);

                        // Stop when you reach the start of the bucket
                        if new_last_index <= bucket_location + Self::BUCKET_CONTENT_OFFSET {
                            break;
                        }
                        new_last_index -= ENTRY_SIZE;
                        // if new_last_index > bucket_location + Self::BUCKET_CONTENT_OFFSET {
                        //     new_last_index -= PARAM_COUNT;
                        // } else {
                        //     break;
                        // }
                    }
                }

                Ok(Success::Value(removed))
            },
            _ => Ok(Success::None),
        }
    }
    //endregion

    //region Resize
    pub fn resize<const ENTRY_SIZE: usize>(storage: &mut SharedStorage) -> Result<ENTRY_SIZE> {
        // println!("------------ RESIZE ------------");
        let nb_buckets = Self::get_nb_buckets(storage);
        let bucket_capacity_elems = Self::get_bucket_capacity_elems(storage);

        let old_hash_table_size = Self::HASH_TABLE_OFFSET + nb_buckets;
        let _old_capacity = old_hash_table_size + nb_buckets * (1 + bucket_capacity_elems * ENTRY_SIZE);

        // Double the number of buckets
        let more_buckets = 2 * nb_buckets;
        let new_hash_table_size = Self::HASH_TABLE_OFFSET + more_buckets;
        let new_capacity = new_hash_table_size + more_buckets * (1 + bucket_capacity_elems * ENTRY_SIZE);

        // If the new size is too large for vm storage return error
        if new_capacity > storage.len() {
            return Err(Error::StorageFull);
        }

        // Prepare the bigger DHashMap
        let mut new_map = vec![0; new_capacity];
        let mut shared_new_map = SharedStorage {
            ptr: new_map.as_mut_ptr(),
            size: new_capacity,
        };

        // TODO no need to add sentinels during init since many of them will be overwritten, add them after the buckets have been filled
        DHashMap::init::<ENTRY_SIZE>(&mut new_map, more_buckets, bucket_capacity_elems);

        // Insert all entries in the new map: ------------------------------------------------------
        // Probe each bucket and append the entries to the new bucket
        for old_bucket_index in 0..nb_buckets {
            let old_bucket_location = storage.get(Self::HASH_TABLE_OFFSET + old_bucket_index) as usize;
            let old_bucket_size = storage.get(old_bucket_location) as usize;

            let mut current_index = (old_bucket_location + 1) as usize;
            let mut moved = 0;

            while moved < old_bucket_size {
                let old_entry_index = current_index;
                let old_entry_end = old_entry_index + ENTRY_SIZE;
                let old_entry = unsafe {
                    slice::from_raw_parts(storage.ptr.add(current_index), ENTRY_SIZE)
                };

                current_index = old_entry_end;

                let entry_key = old_entry[0];
                if entry_key != Self::SENTINEL {
                    // We still have entries to move, we shouldn't reach the end of the bucket
                    assert_ne!(entry_key, DHashMap::LAST);

                    // TODO Store hash with the entry so that we don't need to compute it again?
                    let (
                        _hash,
                        _new_bucket_index,
                        new_bucket_location,
                        _new_bucket_end
                    ) = Self::get_bucket::<ENTRY_SIZE>(entry_key as DKey, &shared_new_map);

                    let new_bucket_size = new_map[new_bucket_location] as usize;
                    if new_bucket_size >= bucket_capacity_elems {
                        return Err(Error::ResizedBucketFull);
                    }

                    // We can add it directly at the end of the new bucket
                    let new_entry_index = (new_bucket_location + 1) + new_bucket_size * ENTRY_SIZE;
                    let new_entry_end = new_entry_index + ENTRY_SIZE;
                    // for offset in 0..PARAM_COUNT {
                    //     new_map[entry_start + offset] = storage.get(old_entry_start + offset);
                    // }

                    let new_entry = &mut new_map[new_entry_index..new_entry_end];
                    new_entry.copy_from_slice(old_entry);

                    new_map[new_bucket_location] += 1;
                    moved += 1;
                }
            }
        }

        unsafe {
            // Put the new map in place of the previous hashmap
            storage.ptr.copy_from_nonoverlapping(new_map.as_ptr(), new_capacity);
        }

        // TODO Just for debug
        // DHashMap::println_ptr::<PARAM_COUNT>(storage.ptr, storage.len());

        Ok(Success::Resized)
    }
    //endregion

    //region Resize and Insert
    pub fn resize_and_insert<const ENTRY_SIZE: usize>(
        key: DKey,
        hash: DHash,
        new_value: &[Word; ENTRY_SIZE],
        storage: &mut SharedStorage
    ) -> Result<ENTRY_SIZE> {
        // println!("------------ RESIZE AND INSERT ---------------");
        let res = DHashMap::resize::<ENTRY_SIZE>(storage);
        match res {
            Ok(Success::Resized) => { /* All good, can proceed */ },
            Err(Error::ResizedBucketFull) | Err(Error::StorageFull) => { return res; },
            _ => { return Err(Error::IllegalState); }
        }

        // Actually insert the new value
        let (
            _hash,
            _bucket_index,
            bucket_location,
            bucket_end
        ) = DHashMap::get_bucket_from_hash::<ENTRY_SIZE>(hash, &storage);

        let retried = DHashMap::insert_entry_in_bucket::<ENTRY_SIZE>(key, bucket_location, bucket_end, &new_value, storage);
        match retried {
            Ok(_) => retried,
            Err(Error::BucketFull) => Err(Error::ResizedBucketFull),
            Err(_) => Err(Error::IllegalState),
        }
    }
    //endregion

    pub fn search<const ENTRY_SIZE: usize>(
        key: DKey,
        bucket_location: usize,
        bucket_end: usize,
        storage: &SharedStorage
    ) -> SearchResult {
        let mut result = SearchResult::BucketFull;
        let mut current_index = bucket_location + Self::BUCKET_CONTENT_OFFSET;

        assert_ne!(key as Word, Self::LAST);
        assert_ne!(key as Word, Self::SENTINEL);

        while current_index < bucket_end {
            match storage.get(current_index) {
                same if same == key as Word => {
                    return SearchResult::Entry(current_index);
                },
                Self::LAST => { return SearchResult::EmptySpot(current_index); },
                Self::SENTINEL => {
                    current_index += ENTRY_SIZE;
                    result = SearchResult::EmptySpot(current_index)
                },
                _other => {
                    current_index += ENTRY_SIZE;
                }
            }
        }

        return result;
    }

    pub fn init<const ENTRY_SIZE: usize>(
        storage: &mut Vec<Word>,
        nb_buckets: usize,
        bucket_capacity_elems: usize,
    ) {

        let bucket_capacity = 1 + bucket_capacity_elems * ENTRY_SIZE;

        storage[0] = nb_buckets as Word;
        storage[1] = bucket_capacity_elems as Word;

        let mut index = Self::HASH_TABLE_OFFSET;
        for bucket_index in 0..nb_buckets {
            let bucket_location = Self::HASH_TABLE_OFFSET + nb_buckets + bucket_index * bucket_capacity;
            storage[index] = bucket_location as Word;
            index += 1;
        }
        for _bucket in 0..nb_buckets {
            storage[index] = 0; // bucket size
            index += 1;
            for _bucket_entry in 0..bucket_capacity_elems {
                storage[index] = DHashMap::LAST;
                index += ENTRY_SIZE;
            }
        }
    }

    //region printing
    pub fn println<const ENTRY_SIZE: usize>(storage: &Vec<Word>) {
        let mut index = 0;

        let nb_buckets = storage[index];
        let bucket_capacity_elems = storage[index + 1];
        println!("<addr {}> {}, {},", index, nb_buckets, bucket_capacity_elems);
        index += 2;

        for _ in 0..nb_buckets {
            // bucket location
            println!("<addr {}> {},", index, storage[index]);
            index += 1;
        }
        for _bucket in 0..nb_buckets {
            println!("Bucket {}:", _bucket);
            // bucket size
            println!("<addr {}> {}", index, storage[index]);
            index += 1;
            for _bucket_entry in 0..bucket_capacity_elems {
                // entry
                print!("<addr {}> ", index);
                for field_index in 0..ENTRY_SIZE {
                    let field = storage[index];
                    if field_index == 0 && field == Self::LAST {
                        print!("LAST, ");
                    } else if field_index == 0 && field == Self::SENTINEL {
                        print!("SENTINEL, ");
                    } else {
                        print!("{}, ", storage[index]);
                    }
                    index += 1;
                }
                println!();
            }
        }
    }

    pub fn print_bucket_sizes<const ENTRY_SIZE: usize>(storage: &Vec<Word>) {
        let nb_buckets = storage[0] as usize;
        let bucket_capacity_elems = storage[1] as usize;
        let mut index = 2 + nb_buckets;

        println!("Bucket sizes (occupancy)");

        let mut total = 0;
        for _bucket in 0..nb_buckets {
            let bucket_size = storage[index];
            total += bucket_size;
            println!("Bucket {}: {}", _bucket, bucket_size);
            index += 1 + bucket_capacity_elems * ENTRY_SIZE;
        }
        println!("Total: {}", total);
    }

    pub fn print_total_size<const ENTRY_SIZE: usize>(storage: &Vec<Word>) {
        let nb_buckets = storage[0] as usize;
        let bucket_capacity_elems = storage[1] as usize;
        let mut index = 2 + nb_buckets;


        let mut total = 0;
        for _bucket in 0..nb_buckets {
            let bucket_size = storage[index];
            total += bucket_size;
            index += 1 + bucket_capacity_elems * ENTRY_SIZE;
        }
        println!("Total size: {}", total);
    }

    pub unsafe fn println_ptr<const ENTRY_SIZE: usize>(storage: *mut Word, storage_size: usize) {
        let v = Vec::from_raw_parts(storage, storage_size, storage_size);
        Self::println::<ENTRY_SIZE>(&v);
        mem::forget(v);
    }
    //endregion
}
/*
let mut map = ThinMap::<u64, u64>::new();
let a = map.get(0);
let c = map.insert(0, 0);
let h = map.remove(0);
let e = map.contains_key(0);
let b = map.len();
let d = map.capacity();
let f = map.is_empty();
let g = map.clear();
let i = map.entry(0);
let j = map.keys();
let h = map.values();
 */

pub enum SearchResult {
    Entry(usize),
    EmptySpot(usize),
    BucketFull
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum Operation {
    Get,    // key
    Insert, //key, value
    Remove, // key
    ContainsKey,    // key
    // Clear    // TODO replace contains by clear since get is enough?
}

impl Operation {
    pub fn corresponding_piece(&self) -> PiecedOperation {
        use Operation::*;
        match self {
            Get => PiecedOperation::GetRequest,
            Insert => PiecedOperation::InsertRequest,
            Remove => PiecedOperation::RemoveRequest,
            ContainsKey => PiecedOperation::HasRequest,
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum PiecedOperation {
    InsertRequest,
    TryInsert,
    ResizeInsert,
    GetRequest,
    Get,
    RemoveRequest,
    Remove,
    HasRequest,
    Has,
}

impl PiecedOperation {
    #[inline]
    pub fn next_operation(&self) -> PiecedOperation {
        use PiecedOperation::*;
        match self {
            InsertRequest => TryInsert,
            GetRequest => Get,
            RemoveRequest => Remove,
            HasRequest => Has,
            _ => panic!("no next operation")
            // TryInsert => ResizeInsert,
            // ResizeInsert => Get,
            // Get => Get,
            // Remove => Get,
            // Has => Get,
        }
    }
}

pub type Result<const ENTRY_SIZE: usize> = core::result::Result<Success<ENTRY_SIZE>, Error>;

#[derive(Debug, Clone, Copy)]
pub enum Success<const ENTRY_SIZE: usize> {
    None,
    Value([Word; ENTRY_SIZE]),
    Inserted,
    Replaced([Word; ENTRY_SIZE]),
    Removed([Word; ENTRY_SIZE]),
    HasKey(bool),
    Resized
}

#[derive(Debug, Clone, Copy)]
pub enum Error {
    KeyNotFound,
    BucketFull,
    StorageFull,
    ResizedBucketFull,
    IllegalState
}