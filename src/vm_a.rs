use std::collections::VecDeque;
use std::mem;
use std::time::Instant;

use anyhow::Result;

use crate::{debug, debugging};
use crate::transaction::Instruction;
use crate::vm::{CPU, ExecutionResult, Executor, Jobs};
use crate::vm_utils::VmStorage;
use crate::wip::{address_translation, Contract, Data, SegmentInstruction, ExternalRequest, WipTransactionResult, Word};

//region Serial VM =================================================================================
pub struct SerialVM {
    pub contracts: Vec<Contract>,
    pub accounts: Vec<Word>,
}

impl SerialVM {
    pub fn new(storage_size: usize) -> Result<Self> {
        let accounts = (0..storage_size).map(|_| 0 as Word).collect();
        let contracts = vec!();
        let vm = Self{ contracts, accounts };
        return Ok(vm);
    }

    pub fn execute(&mut self, mut batch: Vec<ExternalRequest>) -> Result<Vec<WipTransactionResult>> {

        let mut results = Vec::with_capacity(batch.len());

        use WipTransactionResult::*;
        use SegmentInstruction::*;

        for tx in batch.iter() {
            let result = match &tx.data {
                Data::None => {
                    // Transfer of native currency
                    let index_from = address_translation(&tx.from);
                    let balance_from = self.accounts[index_from];
                    if balance_from < tx.amount {
                        Error
                    } else {
                        let index_to = address_translation(&tx.to);
                        self.accounts[index_from] -= tx.amount;
                        self.accounts[index_to] += tx.amount;
                        TransferSuccess
                    }
                },
                Data::NewContract(functions) => {
                    // Creation of a new contract
                    let mut new_contract = Contract{
                        storage: VmStorage::new(0),
                        functions: functions.clone(),
                    };
                    let new_contract_address = self.contracts.len();
                    self.contracts.push(new_contract);
                    Success(new_contract_address)
                },
                Data::Parameters(input_params) => {
                    // Parameters when calling a contract
                    let mut contract = self.contracts.get_mut(tx.to as usize).unwrap();
                    let main_fun = contract.functions.get(0).unwrap();
                    let mut stack = VecDeque::new();
                    let mut params = VecDeque::from(input_params.clone());

                    let mut segment = main_fun.segments.get(0).unwrap();
                    let mut execute = true;
                    let mut segment_result = Error;
                    while execute {
                        for instr in segment.instructions.iter() {
                            match instr {
                                PushParam(index) => {
                                    stack.push_back(params[*index]);
                                },
                                IncrementFromParam(index) => {
                                    let address = params[*index];
                                    let amount = stack.pop_back().unwrap() as Word;
                                    let value = contract.storage.get(address as usize);
                                    contract.storage.set(address as usize, value + amount);
                                },
                                DecrementFromParam(index) => {
                                    let address = params[*index];
                                    let amount = stack.pop_back().unwrap() as Word;
                                    let value = contract.storage.get(address as usize);

                                    if value < amount {
                                        execute = false;
                                        segment_result = Error;
                                        break;
                                    }
                                    contract.storage.set(address as usize, value - amount);
                                },
                                CallSegment(index) => {
                                    segment = main_fun.segments.get(*index).unwrap();
                                    params.truncate(0);
                                    mem::swap(&mut stack, &mut params);
                                    break;
                                },
                                Return(status) => {
                                    segment_result = Success(*status as usize);
                                    execute = false;
                                    break;
                                },
                                _ => todo!(),
                            }
                        }
                    }
                    segment_result
                }
            };

            results.push(result);
        }

        Ok(results)
    }

    pub fn set_account_balance(&mut self, balance: Word) {
        self.accounts.fill(balance);
    }
}

pub struct VMa {
    storage: Vec<Word>
}

impl VMa {
    pub fn new(storage_size: usize) -> Result<Self> {
        let storage = (0..storage_size).map(|_| 0 as Word).collect();
        let vm = Self{ storage };
        return Ok(vm);
    }
}

impl Executor for VMa {
    fn execute(&mut self, mut batch: Jobs) -> Result<Vec<ExecutionResult>> {
let start = Instant::now();
        let mut results = Vec::with_capacity(batch.len());
        let mut backlog = Vec::with_capacity(batch.len());

        let mut stack: VecDeque<Word> = VecDeque::new();

        loop {
            if batch.is_empty() {
debug!("### Done serial execution in {:?}\n", start.elapsed());
                return Ok(results);
            }

            for tx in batch.iter() {
                stack.clear(); // TODO Does this need to be optimised?
                let first_instr = tx.instructions.get(0).unwrap();
                let second_instr = tx.instructions.get(1).unwrap();

                if let Instruction::Increment(addr_inc, amount) = first_instr {
                    let balance_from = *self.storage.get(*addr_inc as usize).unwrap();

                    if let Instruction::Decrement(addr_dec, _) = second_instr {
                        if balance_from >= *amount {
                            *self.storage.get_mut(*addr_inc as usize).unwrap() -= *amount;
                            *self.storage.get_mut(*addr_dec as usize).unwrap() += *amount;
                        }
                    }
                }
                // for instr in tx.instructions.iter() {
                //     CPU::execute_from_array(instr, &mut stack, &mut self.storage);
                // }
                let result = ExecutionResult::todo();
                results.push(result);
                // TODO add to next transaction piece to backlog
            }

            batch.drain(0..).for_each(std::mem::drop);
            mem::swap(&mut batch, &mut backlog);
        }
    }

    fn set_storage(&mut self, value: Word) {
        self.storage.fill(value);
    }
}
//endregion