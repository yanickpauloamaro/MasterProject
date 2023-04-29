use std::cell::{Cell, RefCell};
use strum::EnumIter;
use std::mem;
use rand::seq::SliceRandom;
use std::fmt::Debug;
use std::marker::PhantomData;
use crate::applications::Ballot;
use crate::vm_utils::SharedStorage;
use crate::wip::Word;

pub type SenderAddress = u32;
pub type FunctionAddress = u32;
pub type StaticAddress = u32;
pub type FunctionParameter = u32;

pub const MAX_NB_ADDRESSES: usize = 2;
pub const MAX_NB_PARAMETERS: usize = 2;

// TODO Find safe way to have a variable length array?
#[derive(Clone, Debug, Copy)]
pub struct Transaction<const ADDRESS_COUNT: usize, const PARAM_COUNT: usize> {
    pub sender: SenderAddress,
    pub function: AtomicFunction,
    // pub addresses: BoundedArray<StaticAddress, MAX_NB_ADDRESSES>,
    // pub params: BoundedArray<FunctionParameter, MAX_NB_PARAMETERS>,
    pub addresses: [StaticAddress; ADDRESS_COUNT],
    pub params: [FunctionParameter; PARAM_COUNT],
    // pub nb_addresses: usize,
}

#[repr(u8)]
#[derive(Clone, Copy, Debug, EnumIter)]
pub enum AtomicFunction {
    Transfer = 0,
    // Transfer application split into pieces
    TransferDecrement,
    TransferIncrement,
    Fibonacci,

    Ballot(Ballot),
    // BallotPiece(BallotPieces)

    BestFitStart,
    BestFit,
}

impl AtomicFunction {
    // pub fn index(&self) -> usize {
    //     mem::discriminant(self)
    // }
    pub unsafe fn execute<const ADDRESS_COUNT: usize, const PARAM_COUNT: usize>(
        &self,
        mut tx: Transaction<ADDRESS_COUNT, PARAM_COUNT>,
        mut storage: SharedStorage
    ) -> FunctionResult<ADDRESS_COUNT, PARAM_COUNT> {
        use AtomicFunction::*;
        return match self {
            Transfer => {
                let from = tx.addresses[0] as usize;
                let to = tx.addresses[1] as usize;
                let amount = tx.params[0] as Word;

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
            TransferDecrement => {
                // Example of function that output another function
                let from = tx.addresses[0] as usize;
                let to = tx.params[1] as usize;
                let amount = tx.params[0] as Word;

                if storage.get(from) >= amount {
                    *storage.get_mut(from) -= amount;

                    tx.function = TransferIncrement;
                    tx.addresses[0] = to as StaticAddress;

                    FunctionResult::Another(tx)
                } else {
                    eprintln!("TransferDecrement: Insufficient funds !!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
                    FunctionResult::ErrorMsg("Insufficient funds")
                }
            },
            TransferIncrement => {
                let to = tx.addresses[0] as usize;
                let amount = tx.params[0] as Word;
                *storage.get_mut(to) += amount;
                FunctionResult::Success
            },
            Fibonacci => {
                AtomicFunction::fib(tx.params[0]);
                FunctionResult::Success
            },
            Ballot(piece) => {
                // piece.execute(tx, storage)
                todo!()
            },
            BestFitStart => {
                // let _addr: Vec<_> = addresses.iter().collect();
                // println!("Start range = {:?}", _addr);
                // // addresses: [start_addr..end_addr]
                // // params: []
                // let start_addr = addresses[0];
                // let mut max_addr = start_addr;
                // let mut max = storage.get(start_addr as usize);
                //
                // for addr in addresses.iter() {
                //     let value = storage.get(*addr as usize);
                //     // println!("\t{}->{}", *addr, value);
                //     if value > max {
                //         max = storage.get(max_addr as usize);
                //         max_addr = *addr;
                //     }
                // }
                //
                // // store max for next tx piece
                // tx.params[0] = max as FunctionParameter;
                //
                // // Compute next range
                // let new_starting_addr = start_addr + MAX_NB_ADDRESSES as StaticAddress;
                // let new_end_addr = min(new_starting_addr + MAX_NB_ADDRESSES as StaticAddress - 1, storage.len() as StaticAddress);
                //
                // if new_starting_addr as usize >= storage.len() {
                //     // Commit
                //     return FunctionResult::SuccessValue(max);
                // }
                //
                // //"lock" the max value
                // tx.addresses = BoundedArray::from_range_with(max_addr, new_starting_addr..new_end_addr);
                //
                // tx.function = BestFit;
                // FunctionResult::Another(tx)
                FunctionResult::Success
            },
            BestFit => {
                // let _addr: Vec<_> = addresses.iter().collect();
                // println!("Searching range = {:?}", _addr);
                // // addresses: [old_max_addr, start_addr..end_addr]
                // // params: [old_max]
                // let old_max_addr = addresses[0];
                // let old_max = params[0] as Word;
                // let start_addr = addresses[1];
                //
                // let mut max_addr = old_max_addr;
                // let mut max = old_max;
                //
                // for addr in addresses.iter() {
                //     let value = storage.get(*addr as usize);
                //     if value > max {
                //         max = storage.get(max_addr as usize);
                //         max_addr = *addr;
                //     }
                // }
                //
                // // store max for next tx piece
                // tx.params[0] = max as FunctionParameter;
                //
                // // Compute next range
                // let new_starting_addr = start_addr + MAX_NB_ADDRESSES as StaticAddress - 1;
                // let new_end_addr = min(new_starting_addr + MAX_NB_ADDRESSES as StaticAddress - 1, storage.len() as StaticAddress);
                //
                // if new_starting_addr as usize >= storage.len() {
                //     // Commit
                //     return FunctionResult::SuccessValue(max);
                // }
                //
                // //"lock" the max value
                // tx.addresses = BoundedArray::from_range_with(max_addr, new_starting_addr..new_end_addr);
                //
                // tx.function = BestFit;
                // FunctionResult::Another(tx)
                FunctionResult::Success
            },
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

#[derive(Clone, Copy, Debug)]
pub enum FunctionResult<const ADDRESS_COUNT: usize, const PARAM_COUNT: usize> {
    // Running,
    Success,
    SuccessValue(Word),
    Another(Transaction<ADDRESS_COUNT, PARAM_COUNT>),
    Error,
    ErrorMsg(&'static str)
}

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
        if capacity < size * mem::size_of::<Option<V>>() {
            panic!("Not enough capacity to fit {} elements", size);
        }
        Self{
            base_address,
            capacity,
            size,
            _lifetime: PhantomData,
        }
    }

    pub fn new(base_address: Cell<*mut Option<V>>, size: usize, capacity: usize) -> SharedMap<'a, V> {
        if capacity < size * mem::size_of::<Option<V>>() {
            panic!("Not enough capacity to fit {} elements", size);
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
        if capacity < size * mem::size_of::<Option<V>>() {
            panic!("Not enough capacity to fit {} elements", size);
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
        if key as usize >= self.size { panic!("Access out of range"); }

        let value = self.base_address.get().add(key as usize);
        (*value).as_ref()
    }

    #[inline]
    pub unsafe fn get_mut(&self, key: StaticAddress) -> Option<&'a mut V> {
        if key as usize >= self.size { panic!("Access out of range"); }

        let value = self.base_address.get().add(key as usize);
        (*value).as_mut()
    }

    #[inline]
    pub unsafe fn insert(&self, key: StaticAddress, new_value: V) -> Option<V> {
        if key as usize >= self.size { panic!("Access out of range"); }

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
