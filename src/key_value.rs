use thincollections::thin_map::ThinMap;
use crate::contract::StaticAddress;
use crate::key_value::KeyValueError::KeyNotFound;

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
struct KeyValue {
    inner_map: ThinMap<StaticAddress, u32>
}

enum OperationResult {
    Scan(Vec<u32>),
    Read(u32),
    Write(u32),
    Insert(u32),
    ReadModifyWrite(u32)
}

enum KeyValueError {
    KeyNotFound
}

impl KeyValue {

    pub fn new() -> Self {
        Self { inner_map: ThinMap::new() }
    }

    // TODO Add ReadMany/WriteMany/InsertMany to operate on multiple addresses (explicitly)
    // TODO Use const generic to be able to test with different FIELD_SIZE
    // pub fn read_many(&self, key: StaticAddress, many: [u32; FIELD_SIZE]) -> Result<OperationResult, KeyValueError>{
    //     self.inner_map.get(&key)
    //         .map(|value| OperationResult::Read(*value))
    //         .ok_or(KeyNotFound)
    // }

    pub fn read(&self, key: StaticAddress) -> Result<OperationResult, KeyValueError>{
        self.inner_map.get(&key)
            .map(|value| OperationResult::Read(*value))
            .ok_or(KeyNotFound)
    }

    pub fn write(&mut self, key: StaticAddress, new_value: u32) -> Result<OperationResult, KeyValueError>{
        self.inner_map.get_mut(&key)
            .map(|value| {
                let previous_value = *value;
                *value = new_value;
                OperationResult::Write(previous_value)
            }).ok_or(KeyNotFound)
    }

    pub fn read_modify_write(&mut self, key: StaticAddress, op: fn(u32)->u32) -> Result<OperationResult, KeyValueError>{
        self.inner_map.get_mut(&key)
            .map(|value| {
                let previous_value = *value;
                *value = op(*value);
                OperationResult::ReadModifyWrite(previous_value)
            }).ok_or(KeyNotFound)
    }

    pub fn scan(&self, from: StaticAddress, to: StaticAddress) -> Result<OperationResult, KeyValueError>{
        /* Result should be as large as range...
            => require range to be as long as params?
            => add const RESULT: usize to transactions?
            => put result in a vec?
         */
        let mut result = Vec::with_capacity((to - from) as usize);

        for addr in from..to {
            result.push(*self.inner_map.get(&addr).unwrap_or(&u32::MAX))
        }

        Ok(OperationResult::Scan(result))
    }

    pub fn insert(&mut self, key: StaticAddress, value: u32) -> Result<OperationResult, KeyValueError>{
        let previous_value = self.inner_map.insert(key, value).unwrap_or(u32::MAX);
        Ok(OperationResult::Insert(previous_value))
    }
}