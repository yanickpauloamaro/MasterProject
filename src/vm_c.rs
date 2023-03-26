use std::cmp::max;
use std::collections::VecDeque;
use std::mem;
use std::time::Instant;

use crate::{debug, debugging};
use crate::vm::{ExecutionResult, Executor, Jobs};
use crate::vm_utils::{assign_workers, UNASSIGNED, VmStorage};
use crate::wip::{AssignedWorker, Contract, Data, ExternalRequest, InternalRequest, WipTransactionResult, Word};
use crate::wip::WipTransactionResult::{Error, Pending, Success};
use crate::worker_implementation::WorkerC;

//region Parallel VM crossbeam =====================================================================
pub struct VMc {
    storage: VmStorage,
    nb_workers: usize,
}

impl VMc {
    pub fn new(storage_size: usize, nb_workers: usize, _batch_size: usize) -> anyhow::Result<Self> {

        let storage = VmStorage::new(storage_size);
        let vm = Self{ storage, nb_workers };
        return Ok(vm);
    }
}

impl Executor for VMc {
    fn execute(&mut self, mut batch: Jobs) -> anyhow::Result<Vec<ExecutionResult>> {
let total = Instant::now();
        let mut results = Vec::with_capacity(batch.len());
        let mut backlog = Vec::with_capacity(batch.len());
        let mut address_to_worker = vec![UNASSIGNED; self.storage.len()];

        // return self.execute_rec(results, batch, backlog, address_to_worker);

        loop {
            if batch.is_empty() {
debug!("+++ Total took {:?}\n", total.elapsed());
                return Ok(results);
            }

            // Assign jobs to workers ------------------------------------------------------------------
let a = Instant::now();
            address_to_worker.fill(UNASSIGNED);
            let tx_to_worker = assign_workers(
                self.nb_workers,
                &batch,
                &mut address_to_worker,
                &mut backlog
            );
debug!("+++ Work assignment took {:?}", a.elapsed());
let start = Instant::now();
            // Execute in parallel ----------------------------------------------------------------
            WorkerC::crossbeam(
                self.nb_workers,
                &mut results,
                &mut batch,
                &mut backlog,
                &mut self.storage,
                &tx_to_worker,
            )?;
debug!("+++ Parallel execution in {:?}", start.elapsed());
let end = Instant::now();
            // Prepare next iteration --------------------------------------------------------------
            batch.drain(0..).for_each(std::mem::drop);
            mem::swap(&mut batch, &mut backlog);
debug!("+++ End of loop took {:?}", end.elapsed());
        }
    }

    fn set_storage(&mut self, value: Word) {
        self.storage.set_storage(value);
    }
}
//endregion

pub struct ParallelVM {
    pub contracts: Vec<Contract>,
    pub nb_workers: usize,
}

impl ParallelVM {
    pub fn new(nb_cores: usize) -> anyhow::Result<Self> {
        let contracts = vec!();
        let vm = Self{ contracts, nb_workers: nb_cores };
        return Ok(vm);
    }

    pub fn execute(&mut self, mut requests: Vec<ExternalRequest>) -> anyhow::Result<Vec<WipTransactionResult>> {
        let mut results = vec![Pending; requests.len()];
        let mut batch = Vec::with_capacity(requests.len());

let a = Instant::now();
        self.accept_requests(requests, &mut results, &mut batch);
let b = a.elapsed();
        let mut tx_to_worker = vec![UNASSIGNED; batch.len()];
        // let mut next_worker = 0 as AssignedWorker;
        // let nb_workers = self.nb_workers as AssignedWorker;
        let mut backlog: Vec<InternalRequest> = vec!();

let a = Instant::now();
        self.assign_requests(
            &batch,
            // &mut address_to_worker,
            &mut tx_to_worker,
            self.nb_workers as AssignedWorker,
            &mut backlog
        );

println!("Mapping external into internal took {:?}", b);
println!("Assignment took {:?}", a.elapsed());

        Ok(results)
    }

    pub fn accept_requests(
        &mut self,
        requests: Vec<ExternalRequest>,
        results: &mut Vec<WipTransactionResult>,
        batch: &mut Vec<InternalRequest>
    )
    {
        for (request_index, request) in requests.iter().enumerate() {
            match &request.data {
                Data::None => {
                    // Transfer of native currency
                    batch.push(InternalRequest{
                        request_index,
                        contract_index: 0,
                        function_index: 0,
                        segment_index: 0,
                        params: vec!(request.from, request.amount, request.to),
                    });
                },
                Data::NewContract(functions) => {
                    // Creation of a new contract
                    let mut new_contract = Contract{
                        storage: VmStorage::new(0),
                        functions: functions.clone(),
                    };
                    let new_contract_address = self.contracts.len();
                    self.contracts.push(new_contract);

                    results[request_index] = Success(new_contract_address);
                },
                Data::Parameters(params) => {
                    // TODO Should be able to call other functions?
                    batch.push(InternalRequest{
                        request_index,
                        contract_index: 0,
                        function_index: 0,
                        segment_index: 0,
                        params: params.clone(),
                    });
                }
            }
        }
    }

    pub fn assign_requests(
        &mut self,
        batch: &Vec<InternalRequest>,
        // address_to_worker: &mut Vec<AssignedWorker>,
        tx_to_worker: &mut Vec<AssignedWorker>,
        nb_workers: AssignedWorker,
        backlog: &mut Vec<InternalRequest>
    ) {
        // TODO For now assume there is only one contract: the native transfer
        let contract = self.contracts.get(0).unwrap();
        let mut address_to_worker = vec![UNASSIGNED; contract.storage.len()];

        let mut next_worker = 0 as AssignedWorker;

        'outer: for (req_index, req) in batch.iter().enumerate() {
            let function = contract.functions.get(req.function_index).unwrap();
            let segment = function.segments.get(req.segment_index).unwrap();
            let accesses = segment.accessed_addresses(&req.params);

            let mut current_assigned = UNASSIGNED;
            // println!("Tx {} accesses: {:?}", req_index, accesses);
            let a = Instant::now();
            'inner: for access in accesses.iter() {
                let assigned = address_to_worker[*access];
                if assigned == UNASSIGNED {
                    current_assigned = max(assigned, current_assigned);
                } else if assigned != current_assigned {
                    backlog.push(req.clone());
                    continue 'outer;
                }
            }
            let b = a.elapsed();
            if current_assigned == UNASSIGNED {
                current_assigned = next_worker + 1;
                next_worker = if next_worker == nb_workers - 1 {
                    0
                } else {
                    next_worker + 1
                };
            }

            let c = Instant::now();
            tx_to_worker[req_index] = current_assigned;
            for access in accesses.iter() {
                address_to_worker[*access] = current_assigned;
            }
            // println!("Tx {}: \n\tfirst loop: {:?}, \n\tsecond loop: {:?}", req_index, b, c.elapsed());
        }
    }
}
