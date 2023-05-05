use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::mem;
use either::Either;
use thincollections::thin_map::ThinMap;
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

    pub const LAST: Word = Word::MAX;
    pub const SENTINEL: Word = Self::LAST - 1;

    pub fn search_bucket<const ENTRY_SIZE: usize>(
        key: DKey,
        bucket_start: usize,
        bucket_end: usize,
        storage: &mut SharedStorage
    ) -> SearchResult {
        let mut result = SearchResult::BucketFull;
        let mut current_index = bucket_start;

        assert_ne!(key as Word, Self::LAST);
        assert_ne!(key as Word, Self::SENTINEL);

        println!("Searching key {} (bucket start = {}, bucket end = {})", key, bucket_start, bucket_end);
        while current_index < bucket_end {
            let entry_ptr = storage.get_mut(current_index);
            println!("\tSearching at index {}", current_index);
            match storage.get(current_index) {
                same if same == key as Word => {
                    return SearchResult::Entry(entry_ptr, current_index);
                },
                Self::LAST => { return SearchResult::EmptySpot(entry_ptr, current_index); },
                Self::SENTINEL => {
                    current_index += ENTRY_SIZE;
                    result = SearchResult::EmptySpot(entry_ptr, current_index)
                },
                _other => {
                    current_index += ENTRY_SIZE;
                }
            }
        }

        return result;
    }

    pub fn println<const ENTRY_SIZE: usize>(storage: &Vec<Word>, nb_buckets: usize, bucket_capacity: usize) {
        let mut index = 0;
        // nb_buckets and bucket_capacity
        println!("<addr {}> {}, {},", index, storage[index], storage[index + 1]);
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
            for _bucket_entry in 0..bucket_capacity {
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

    pub unsafe fn println_ptr<const ENTRY_SIZE: usize>(storage: *mut Word, storage_size: usize, nb_buckets: usize, bucket_capacity: usize) {
        let v = Vec::from_raw_parts(storage, storage_size, storage_size);
        Self::println::<ENTRY_SIZE>(&v, nb_buckets, bucket_capacity);
        mem::forget(v);
    }
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
    Entry(*mut Word, usize),
    EmptySpot(*mut Word, usize),
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

// TODO Choose content type (u32 or u64)
#[derive(Debug, Clone, Copy)]
pub enum Success<const ENTRY_SIZE: usize> {
    None,
    Value([Word; ENTRY_SIZE]),
    Inserted,
    Replaced([Word; ENTRY_SIZE]),
    Removed([Word; ENTRY_SIZE]),
    HasKey(bool)
}

#[derive(Debug, Clone, Copy)]
pub enum Error {
    KeyNotFound
}