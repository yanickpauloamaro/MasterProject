use std::fmt::Debug;
use crate::contract::{SharedMap, StaticAddress};
use crate::key_value::KeyValueError::KeyNotFound;
use crate::wip::Word;
/* Memory layout
[base_address] capacity
[base_address + key_0_address] value_0_word_0
[base_address + key_0_address + i] value_0_word_i
[base_address + key_0_address + FIELD_SIZE - 1] value_0_last_word
...
[base_address + key_j_address + k] value_j_word_k
...
Notes to make the workload
# keys must be less than storage_size
# key_i_address + FIELD_SIZE - 1 < key_j_address

 */
type Value = Word;
pub struct KeyValue<'a> {
    pub inner_map: SharedMap<'a, Value>
}
#[derive(Debug, Clone, Copy)]
pub enum KeyValueOperation {
    Read,
    Write,
    ReadModifyWrite,
    Scan,
    Insert,
}
#[derive(Debug, Clone)]
pub enum OperationResult {
    Scan(Vec<Value>),
    Read(Value),
    Write(Value),
    Insert(Value),
    InsertNew,
    ReadModifyWrite(Value)
}
#[derive(Debug, Clone, Copy)]
pub enum KeyValueError {
    KeyNotFound
}

impl<'a> KeyValue<'a> {

    // TODO Add ReadMany/WriteMany/InsertMany to operate on multiple addresses (explicitly)
    // TODO Use const generic to be able to test with different FIELD_SIZE or use slices
    // pub fn read_many(&self, key: StaticAddress, many: [u32; FIELD_SIZE]) -> Result<OperationResult, KeyValueError>{
    //     self.inner_map.get(&key)
    //         .map(|value| OperationResult::Read(*value))
    //         .ok_or(KeyNotFound)
    // }

    pub unsafe fn read(&self, key: StaticAddress) -> Result<OperationResult, KeyValueError>{
        self.inner_map.get(key)
            .map(|value| OperationResult::Read(*value))
            .ok_or(KeyNotFound)
    }

    pub unsafe fn write(&mut self, key: StaticAddress, new_value: Value) -> Result<OperationResult, KeyValueError>{
        // TODO Could use inner_map.insert
        self.inner_map.get_mut(key)
            .map(|value| {
                let previous_value = *value;
                *value = new_value;
                OperationResult::Write(previous_value)
            }).ok_or(KeyNotFound)
    }

    pub unsafe fn read_modify_write(&mut self, key: StaticAddress, op: fn(Value)->Value) -> Result<OperationResult, KeyValueError>{
        self.inner_map.get_mut(key)
            .map(|value| {
                let previous_value = *value;
                *value = op(*value);
                OperationResult::ReadModifyWrite(previous_value)
            }).ok_or(KeyNotFound)
    }

    pub unsafe fn scan(&self, from: StaticAddress, to: StaticAddress) -> Result<OperationResult, KeyValueError>{
        /* Result should be as large as range...
            => require range to be as long as params?
            => add const generic RESULT: usize to transactions?
            => put result in a vec? (easiest solution)
         */
        let mut result = Vec::with_capacity((to - from) as usize);

        for addr in from..to {
            result.push(*self.inner_map.get(addr).unwrap_or(&Value::MAX))
        }

        Ok(OperationResult::Scan(result))
    }

    pub unsafe fn insert(&mut self, key: StaticAddress, value: Value) -> Result<OperationResult, KeyValueError>{
        let res = match self.inner_map.insert(key, value) {
            Some(previous_value) => OperationResult::Insert(previous_value),
            None => OperationResult::InsertNew,
        };

        Ok(res)
    }
}