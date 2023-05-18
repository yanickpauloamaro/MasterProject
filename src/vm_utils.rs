use std::cell::RefCell;
use std::cmp::{max, min};
use std::collections::HashMap;
use std::fmt::format;
use std::hash::BuildHasherDefault;
use std::{cmp, iter, mem};
use std::ops::Range;
use std::slice::Iter;
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
// use std::sync::mpsc::Receiver;
use std::time::{Instant, Duration, SystemTime, UNIX_EPOCH};
use std::vec::Drain;

use ahash::AHasher;
use anyhow::{anyhow, Context};
use core_affinity::CoreId;
use crossbeam::channel::{Receiver, Sender as CrossSender, unbounded};
use futures::AsyncReadExt;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use thincollections::thin_set::ThinSet;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, Sender as TokioSender};
use tokio::task::{JoinHandle as TokioHandle, spawn_blocking};
// use tokio::time::Instant;

use crate::config::RunParameter;
use crate::contract::{AccessPattern, AccessType, AtomicFunction, FunctionResult, StaticAddress, Transaction, TransactionType};
use crate::contract::FunctionResult::Another;
use crate::key_value::KeyValueOperation;
use crate::parallel_vm::Job;
use crate::utils::mean_ci_str;
use crate::vm::{Executor, Jobs};
use crate::vm_a::VMa;
use crate::vm_b::VMb;
use crate::vm_c::VMc;
use crate::wip::{AssignedWorker, Word};
use crate::worker_implementation::{WorkerBStd, WorkerBTokio};

pub struct VmUtils;
impl VmUtils {
    #[inline]
    pub fn timestamp(str: &str) {
        // let since_unix = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        // eprintln!("{:.9?}: {}", since_unix, str);
    }

    #[inline]
    pub fn took(str: &str, time: Duration) {
        // eprintln!("{} took {:?}", str, time);
    }

    #[inline]
    pub fn nb_rounds(nb_rounds: usize) {
        // eprintln!("Took {} execution rounds", nb_rounds);
    }

    #[inline]
    pub fn split<const A: usize, const P: usize>(
        backlog: &mut Vec<Transaction<A, P>>,
        recipients: &Vec<CrossSender<Vec<Transaction<A, P>>>>,
        name: &str,
    ) -> (Duration, Duration) {
        let chunk_size = (backlog.len() / recipients.len()) + 1;
        // eprintln!("Sending backlog to {}s ({} tx)", name, backlog.len());
        let mut work = backlog.drain(..);
        let mut allocation = Duration::from_secs(0);
        let mut sending = Duration::from_secs(0);
        for (index, recipient) in recipients.iter().enumerate() {
            // let a = Instant::now();
            let chunk = work.by_ref().take(chunk_size).collect_vec();
            // allocation += a.elapsed();
            // eprintln!("\tSending chunk to {} {} ({} tx)", name, index, chunk.len());
            // let a = Instant::now();
            recipient.send(chunk)
                .expect(format!("Failed to send transactions to {} {}", name, index).as_str());
            // sending += a.elapsed();
        }
        // if name == "scheduler" {
        //     println!("Allocation took {:?}", allocation);
        // }
        (allocation, sending)
    }

    // fn executor_pool<const A: usize, const P: usize>(
    //     to_executors: &Vec<CrossSender<Vec<Transaction<A, P>>>>,
    //     from_executors: &Vec<Receiver<Vec<FunctionResult<A, P>>>>,
    //     schedule: &mut SchedulerOutput<A, P>,
    //     backlog: &mut Vec<usize>,
    //     results: &mut Vec<FunctionResult<A, P>>,
    //     mut __executor_msg_allocation: &mut Duration,
    //     mut __executor_msg_sending: &mut Duration,
    // ) -> usize {
    //     let mut nb_executed = 0;
    //     // eprintln!("Coordinator: Sending backlog to executors");
    //     // eprintln!("---------------------------");
    //     VmUtils::timestamp("Coordinator sending backlog to executors");
    //
    //     let (alloc, sending) = VmUtils::split::<A, P>(schedule, to_executors, "executor");
    //     *__executor_msg_allocation += alloc;
    //     *__executor_msg_sending += sending;
    //     assert!(schedule.is_empty());
    //
    //     // TODO Execute your own chunk ------------------------------------------------------
    //
    //     // Receive result from other executors -----------------------------------------
    //     for (executor_index, executor) in from_executors.iter().enumerate() {
    //         // eprintln!("Coordinator: Waiting for results from executor {}", executor_index);
    //         if let Ok(mut results_and_tx) = executor.recv() {
    //             // eprintln!("\tGot result ({:?} results)", results_and_tx.len());
    //             for res in results_and_tx.drain(..) {
    //                 match res {
    //                     Another(tx) => backlog.push(tx),
    //                     res => {
    //                         nb_executed += 1;
    //                         results.push(res)
    //                     },
    //                 }
    //             }
    //             // Done with this executor result (could reuse vec)
    //         } else {
    //             panic!("Failed to receive executor result")
    //         }
    //         // Done with this executor
    //     }
    //
    //     nb_executed
    // }
}

pub enum Assignment {
    Read,
    Write(usize)
}

pub struct Scheduling;
type Map = HashMap<StaticAddress, AccessType, BuildHasherDefault<AHasher>>;
impl Scheduling {
    pub fn schedule_chunk_old<const A: usize, const P: usize>(
        mut chunk: Vec<Transaction<A, P>>,
        mut scheduled: Vec<Transaction<A, P>>,
        mut postponed: Vec<Transaction<A, P>>,
        mut working_set: AddressSet,
        // a: Instant,
    ) -> (Vec<Transaction<A, P>>, Vec<Transaction<A, P>>)
    {
        // let init_duration = a.elapsed();
        // let mut duration = Duration::from_secs(0);
        'outer: for tx in chunk {
            if tx.function != AtomicFunction::KeyValue(KeyValueOperation::Scan) {
                // let start = Instant::now();
                for addr in tx.addresses.iter() {
                    if !working_set.insert(*addr) {
                        // Can't add tx to schedule
                        postponed.push(tx);
                        // duration += start.elapsed();
                        continue 'outer;
                    }
                }
                // duration += start.elapsed();
            } else {
                // eprintln!("Processing a scan operation");
                for addr in tx.addresses[0]..tx.addresses[1] {
                    if !working_set.insert(addr) {
                        // Can't add tx to schedule
                        postponed.push(tx);
                        continue 'outer;
                    }
                }
            }
            scheduled.push(tx);
        }

        // total, init, read+writes
        // println!("\ttook {:?} ({} µs, {} µs)", a.elapsed(), init_duration.as_micros(), duration.as_micros());
        (scheduled, postponed)
    }

    pub fn schedule_chunk_assign<const A: usize, const P: usize>(
        address_map: &mut HashMap<StaticAddress, Assignment, BuildHasherDefault<AHasher>>,
        new_reads: &mut Vec<StaticAddress>,
        new_writes: &mut Vec<StaticAddress>,
        schedule: &mut Schedule<A, P>
    )
    {
        let mut round_robin = 0;
        let nb_workers = schedule.assignments.len();

        'backlog: for (tx_index, tx) in schedule.transactions.iter().enumerate() {

            match tx.tpe() {
                TransactionType::ReadOnly => {
                    schedule.read_only.push(tx_index);
                    continue;
                },
                TransactionType:: Exclusive => {
                    schedule.exclusive.push(tx_index);
                    continue;
                },
                TransactionType::Writes => { }
            }


            let (possible_reads, possible_writes) = tx.accesses();
            let reads = possible_reads.as_ref().map_or([].as_slice(), |inside| inside.as_slice());
            let writes = possible_writes.as_ref().map_or([].as_slice(), |inside| inside.as_slice());

            new_reads.truncate(0);
            'reads: for pattern in reads {
                if let AccessPattern::Done = pattern {
                    // No more reads, start processing writes
                    break 'reads;
                }

                // In this version, AccessPattern::All would mean TransactionType is Exclusive
                // So the tx would have already been scheduled by now

                for addr in pattern.addresses() {
                    match address_map.get(&addr) {
                        None => {
                            // This is a new read, will need to insert in address map
                            new_reads.push(addr);
                        },
                        Some(Assignment::Read) => {
                            // This address was already read by another tx, nothing to do
                        },
                        Some(Assignment::Write(_w_i)) => {
                            // Another tx already wrote to this address, can't read concurrently
                            /* TODO Technically if this tx doesn't access addresses assigned to workers
                                other than _w_i (for both reads and writes), then it could be assigned
                                to _w_i. Would need to check for conflict once all other txs have been
                                scheduled
                             */
                            schedule.postponed.push(tx_index);
                            continue 'backlog;
                        },
                    }
                }
            }

            let mut assigned_worker = None;

            new_writes.truncate(0);
            'writes: for pattern in writes {
                if let AccessPattern::Done = pattern {
                    // No more writes
                    break 'writes;
                }

                // In this version, AccessPattern::All would mean TransactionType is Exclusive
                // So the tx would have already been scheduled by now

                for addr in pattern.addresses() {
                    match address_map.get(&addr) {
                        /* Cases (1) and (2):
                            This tx writes some addresses that are new and come that were already written
                            and are therefore assigned to some worker w_i.
                            Assigning this tx to w_i is risky because it might have a cascading effect
                            and lead to a worst case schedule transactions all overlap each other.
                            For example in the case of a loop in account transfer:
                            transactions: A->B, B->C, C->D, D->E, E->F, F->A

                            If we don't prevent assignment to propagate we would get the schedule:
                            w_0: A->B, B->C, C->D, D->E, E->F, F->A
                            postponed: none
                            which does not allow any parallelism

                            But if we prevent it, we get:
                            w_0: A->B,
                            w_1: C->D,
                            w_2: E->F,
                            postponed: B->C, D->E, F->A
                            -----
                            w_0: B->C,
                            w_1: D->E,
                            w_2: F->A,
                            postponed: none
                         */
                        None if assigned_worker.is_some() => {
                            // This is a new write, but this tx is already assigned.
                            // c.f. case (1)
                            schedule.postponed.push(tx_index);
                            continue 'backlog;

                            /* TODO just for dev, this indeed assign all txs to a single worker in the case
                                of a transaction loop

                                new_writes.push(addr);
                             */
                        },
                        Some(Assignment::Write(_w_i)) if !new_writes.is_empty() => {
                            // This address has already been written to, but can't be assigned to this worker
                            // c.f. case (2)
                            schedule.postponed.push(tx_index);
                            continue 'backlog;

                            /* TODO just for dev, this indeed assign all txs to a single worker in the case
                                of a transaction loop

                                let assigned = assigned_worker.get_or_insert(*_w_i);
                                if assigned != _w_i {
                                    // This tx accesses addresses written by different workers, it
                                    // can't be added to this schedule
                                    postponed.push(tx_index);
                                    continue 'backlog;
                                }
                            */
                        }
                        None => {
                            // This is a new write, will need to insert in address map
                            new_writes.push(addr);
                        },
                        Some(Assignment::Read) => {
                            // Another tx already read this address, can't write concurrently
                            /* TODO Technically if the tx that read this addr are all assigned to
                                the same worker, it might be possible to assign this tx to it
                                worker as well. Would need to include info about the reader(s) in the
                                assignment. Could check at the end if there are any other conflicts
                                (e.g. writes/reads to an address assigned to someone else
                             */
                            schedule.postponed.push(tx_index);
                            continue 'backlog;
                        },
                        Some(Assignment::Write(w_i)) => {
                            // None => now assign to w_i
                            // w_i => ok
                            // w_j => conflict
                            // This also assigns the tx to w_i if it wasn't already assigned
                            let assigned = assigned_worker.get_or_insert(*w_i);
                            if assigned != w_i {
                                // This tx accesses addresses written by different workers, it
                                // can't be added to this schedule
                                schedule.postponed.push(tx_index);
                                continue 'backlog;
                            }
                        }
                    }
                }
            }

            let worker_index = match assigned_worker {
                Some(w_i) => w_i,
                None => {
                    // All writes access new addresses, assign a new worker (round robin)
                    /* TODO Could try to assign to the worker with the smallest backlog
                        This would mean putting all txs that can be assigned to any worker in a separate
                        list and so that, at the end, they can be assigned to worker based on workload
                     */
                    let w_i = round_robin;
                    if round_robin == nb_workers - 1 {
                        round_robin = 0;
                    } else {
                        round_robin += 1;
                    }
                    w_i
                }
            };

            // This tx can be added to the schedule
            schedule.assignments[worker_index].push(tx_index);
            schedule.nb_assigned_tx += 1;

            // Don't forget to update the address map
            for addr in new_reads.drain(..) {
                address_map.insert(addr, Assignment::Read);
            }
            for addr in new_writes.drain(..) {
                address_map.insert(addr, Assignment::Write(worker_index));
            }
        }

        // todo!("return something?")
    }

    pub fn schedule_chunk_new<const A: usize, const P: usize>(
        transactions: &Arc<Vec<Transaction<A, P>>>,
        mut chunk: &mut Vec<usize>,
        mut scheduled: &mut Vec<usize>,
        mut postponed: &mut Vec<usize>,
        mut address_map: &mut Map,
        // a: Instant,
    )
    {

        assert!(!chunk.is_empty());
        if chunk.len() == 1 {
            scheduled.push(chunk.pop().unwrap());
            return;
        }

        let mut remainder = 0;

        let mut some_reads = false;
        let mut some_writes = false;

        let mut read_locked = false;
        let mut write_locked = false;

        let can_read = |addr: StaticAddress, map: &mut Map| {
            let entry = map.entry(addr).or_insert(AccessType::Read);
            *entry == AccessType::Read
        };
        let can_write = |addr: StaticAddress, map: &mut Map| {
            let entry = map.entry(addr).or_insert(AccessType::TryWrite);
            if *entry == AccessType::TryWrite {
                *entry = AccessType::Write;
                true
            } else {
                false
            }
        };

        // let init_duration = a.elapsed();
        // let mut base_case_duration = Duration::from_secs(0);
        // let mut reads_duration = Duration::from_secs(0);
        // let mut writes_duration = Duration::from_secs(0);

        'backlog: for tx_index in chunk.drain(0..) {
            let tx = transactions.get(tx_index).unwrap();
            remainder += 1;
            // let base_case_start = Instant::now();
            let (possible_reads, possible_writes) = tx.accesses();
            let reads = possible_reads.as_ref().map_or([].as_slice(), |inside| inside.as_slice());
            let writes = possible_writes.as_ref().map_or([].as_slice(), |inside| inside.as_slice());

            // Tx without any memory accesses ------------------------------------------------------
            if reads.is_empty() && writes.is_empty() {
                scheduled.push(tx_index);
                continue 'backlog;
            }

            // Only reads are allowed --------------------------------------------------------------
            if read_locked {
                if writes.is_empty() { scheduled.push(tx_index); }
                else { postponed.push(tx_index); }
                continue 'backlog;
            }

            // All transactions with accesses will be postponed ------------------------------------
            if write_locked {
                postponed.push(tx_index);
                continue 'backlog;
            }
            // base_case_duration += base_case_start.elapsed();

            // NB: A tx might be postponed after having some of its addresses added to the address set
            // It is probably too expensive to rollback those changes. TODO Check
            // Start with reads because they are less problematic if the tx is postponed
            // Process reads -----------------------------------------------------------------------
            // let read_start = Instant::now();
            'reads: for read in reads {
                match read {
                    AccessPattern::Address(addr) => {
                        some_reads = true;
                        if !can_read(*addr, &mut address_map) {
                            postponed.push(tx_index);
                            // reads_duration += read_start.elapsed();
                            continue 'backlog;
                        }
                    },
                    AccessPattern::Range(from, to) => {
                        some_reads = true;
                        assert!(from < to);
                        'range: for addr in (*from)..(*to) {
                            if !can_read(addr, &mut address_map) {
                                postponed.push(tx_index);
                                // reads_duration += read_start.elapsed();
                                continue 'backlog;
                            }
                        }
                    },
                    AccessPattern::All => {
                        if some_writes {
                            postponed.push(tx_index);
                        } else {
                            scheduled.push(tx_index);
                            read_locked = true;
                        }
                        // Read-all transactions can't have any other accesses, can proceed with the next tx
                        // reads_duration += read_start.elapsed();
                        continue 'backlog;
                    },
                    AccessPattern::Done => {
                        // reads_duration += read_start.elapsed();
                        break 'reads;
                    }
                }
            }
            // reads_duration += read_start.elapsed();

            // Process writes ----------------------------------------------------------------------
            // let writes_start = Instant::now();
            'writes: for write in writes {
                match write {
                    AccessPattern::Address(addr) => {
                        some_writes = true;
                        if !can_write(*addr, &mut address_map) {
                            postponed.push(tx_index);
                            // writes_duration += writes_start.elapsed();
                            continue 'backlog;
                        }
                    },
                    AccessPattern::Range(from, to) => {
                        some_writes = true;
                        assert!(from < to);
                        'range: for addr in (*from)..(*to) {
                            if !can_write(addr, &mut address_map) {
                                postponed.push(tx_index);
                                // writes_duration += writes_start.elapsed();
                                continue 'backlog;
                            }
                        }
                    },
                    AccessPattern::All => {
                        if some_reads || some_writes {
                            postponed.push(tx_index);
                        } else {
                            scheduled.push(tx_index);
                            write_locked = true;
                        }

                        // Write-all transactions can't have any other accesses, can proceed with the next tx
                        continue 'backlog;
                    },
                    AccessPattern::Done => {
                        // writes_duration += writes_start.elapsed();
                        break 'writes;
                    }
                }
            }
            // writes_duration += writes_start.elapsed();

            // Transaction can be executed in parallel
            scheduled.push(tx_index);
        }

        // let total = a.elapsed();
        // println!("Scheduled {:?}", scheduled);
        // println!("Scheduled: {}, postponed: {}, took {:?}", scheduled.len(), postponed.len(), total);
        // total, (init, base_case, reads, writes)
        // println!("\ttook {:.3?} \t({:?} µs, {:?} µs, {:?} µs, {:?} µs)", total, init_duration.as_micros(), base_case_duration.as_micros(), reads_duration.as_micros(), writes_duration.as_micros());
        // (scheduled, postponed)
    }
}

struct Execution;
impl Execution {
    #[inline]
    pub fn execute_tx<const A: usize, const P: usize>(
        schedule: &Arc<Schedule<A, P>>,
        index_in_schedule: usize,
        results: &mut Vec<(usize, FunctionResult<A, P>)>,
        new_transactions: &mut Vec<Transaction<A, P>>,
        storage: SharedStorage
    ) {
        let tx = schedule.transactions[index_in_schedule].clone();
        let tx_index = tx.tx_index;
        let function = tx.function;
        unsafe {
            match function.execute(tx, storage) {
                Another(generated_tx) => {
                    new_transactions.push(generated_tx)
                },
                res => {
                    // TODO would it be better to send the results directly?
                    results.push((tx_index, res))
                }
            }
        }
    }

    #[inline]
    pub fn execute_read_only<const A: usize, const P: usize>(
        schedules: &Vec<Arc<Schedule<A, P>>>,
        executor_index: usize,
        nb_executors: usize,
        results: &mut Vec<(usize, FunctionResult<A, P>)>,
        new_transactions: &mut Vec<Transaction<A, P>>,
        storage: SharedStorage
    ) {

        let read_only_indices = schedules.iter()
            .enumerate()
            .flat_map(|(schedule_index, schedule)| {
                schedule.read_only.iter()
                    .map(move |index_in_schedule| (schedule_index, *index_in_schedule))
            });

        let assigned = read_only_indices
            .dropping(executor_index)
            .step_by(nb_executors);
        for (schedule_index, index_in_schedule) in assigned {
            let schedule = &schedules[schedule_index];
            Execution::execute_tx(
                schedule,
                index_in_schedule,
                results,
                new_transactions,
                storage.clone()
            )
        }
    }

    #[inline]
    pub fn execute_assigned<const A: usize, const P: usize>(
        schedules: &Vec<Arc<Schedule<A, P>>>,
        executor_index: usize,
        schedule_index: usize,
        results: &mut Vec<(usize, FunctionResult<A, P>)>,
        new_transactions: &mut Vec<Transaction<A, P>>,
        storage: SharedStorage
    ) {
        let schedule = &schedules[schedule_index];
        let assigned = &schedule.assignments[executor_index];

        for index_in_schedule in assigned {
            Execution::execute_tx(
                schedule,
                *index_in_schedule,
                results,
                new_transactions,
                storage
            )
        }
    }

    #[inline]
    pub fn execute_exclusive<const A: usize, const P: usize>(
        schedules: &Vec<Arc<Schedule<A, P>>>,
        results: &mut Vec<(usize, FunctionResult<A, P>)>,
        new_transactions: &mut Vec<Transaction<A, P>>,
        storage: SharedStorage
    ) {

        for schedule in schedules.iter() {
            for index_in_schedule in &schedule.exclusive {
                Execution::execute_tx(
                    schedule,
                    *index_in_schedule,
                    results,
                    new_transactions,
                    storage
                )
            }
        }
    }
}

#[derive(Debug)]
pub struct SchedulerInput<const A: usize, const P: usize> {
    pub transactions: Arc<Vec<Transaction<A, P>>>,
    pub range: Range<usize>
}
#[derive(Debug, Clone)]
pub struct SchedulerOutput<const A: usize, const P: usize> {
    pub transactions: Arc<Vec<Transaction<A, P>>>,
    pub scheduled: Arc<Vec<usize>>
}
pub struct SchedulingCore<const A: usize, const P: usize> {
    scheduler_index: usize,
    address_map: RefCell<Map>,
    postponed: RefCell<Vec<usize>>,
    backlog: RefCell<Vec<usize>>,
    output: CrossSender<SchedulerOutput<A, P>>,
    duration: Arc<Mutex<Duration>>
}
// enum SchedulerOutput{
//     Schedule(Range<usize>),
//     Exclusive(usize),
//
// }
impl<const A: usize, const P: usize> SchedulingCore<A, P> {
    pub fn spawn(
        scheduler_index: usize,
        chunk_size: usize,
        input: Receiver<SchedulerInput<A, P>>,
        output: CrossSender<SchedulerOutput<A, P>>,
        duration: Arc<Mutex<Duration>>
    ) -> JoinHandle<anyhow::Result<()>> {
        std::thread::spawn(move || {

            let pinned = core_affinity::set_for_current(CoreId{ id: scheduler_index });
            if !pinned {
                return Err(anyhow!("Unable to pin to scheduler core to CPU #{}", scheduler_index));
            }

            let mut address_map = HashMap::with_capacity_and_hasher(A * chunk_size, BuildHasherDefault::default());
            let mut postponed = Vec::with_capacity(chunk_size);
            let mut backlog = Vec::with_capacity(chunk_size);

            let mut core = Self{
                scheduler_index,
                address_map: RefCell::new(address_map),
                postponed: RefCell::new(postponed),
                backlog: RefCell::new(backlog),
                output,
                duration
            };

            while let Ok(mut received) = input.recv() {

                let a = Instant::now();
                core.schedule(received);
                *core.duration.lock().unwrap() += a.elapsed();
            }

            Ok(())
        })
    }

    fn schedule(&mut self, mut received: SchedulerInput<A, P>) {
        // let mut scheduled = Vec::with_capacity(transactions.len());
        // for index in range {
        //     // schedule tx
        //     // send schedule
        // }
        // mem::swap(backlog, postponed);
        // self.address_map.get_mut().clear();

        // VmUtils::timestamp(format!("Scheduler {} starts scheduling", self.scheduler_index).as_str());

        let SchedulerInput{ transactions, range } = received;
        let mut backlog = self.backlog.get_mut();
        backlog.extend(range);

        while !backlog.is_empty() {
            // TODO Remove this alloc too
            let mut scheduled = Vec::with_capacity(backlog.len());

            Scheduling::schedule_chunk_new(
                &transactions,
                backlog,
                &mut scheduled,
                self.postponed.get_mut(),
                self.address_map.get_mut(),
                // c
            );
            if scheduled.is_empty() { panic!("Scheduler produced an empty schedule"); }
            // backlog is now empty

            // VmUtils::timestamp(format!("\tScheduler {} sends schedule", self.scheduler_index).as_str());
            if let Err(e) = self.output.send(
                SchedulerOutput{ transactions: transactions.clone(), scheduled: Arc::new(scheduled) }
            ) {
                panic!("Failed to send schedule: {:?}", e.into_inner());
            }

            mem::swap(backlog, self.postponed.get_mut());
            self.address_map.get_mut().clear();
        }

        if let Err(e) = self.output.send(
            SchedulerOutput{ transactions: transactions.clone(), scheduled: Arc::new(vec!()) }
        ) {
            panic!("Failed to send end: {:?}", e.into_inner());
        }

        VmUtils::timestamp(format!("Scheduler {} done scheduling", self.scheduler_index).as_str());
    }
}

#[derive(Clone, Debug)]
pub struct ExecutorInput<const A: usize, const P: usize> {
    pub scheduler_output: SchedulerOutput<A, P>,
    pub indices_range: Range<usize>,
}

pub struct ExecutionCore<const A: usize, const P: usize> {
    pub executor_index: usize,
    pub shared_storage: SharedStorage,
    pub output: CrossSender<Vec<FunctionResult<A, P>>>,
    pub duration: Arc<Mutex<Duration>>,
    pub new_txs: Arc<Mutex<Vec<Transaction<A, P>>>>
}
impl<const A: usize, const P: usize> ExecutionCore<A, P> {

    pub fn spawn(
        executor_index: usize,
        input: Receiver<ExecutorInput<A, P>>,
        output: CrossSender<Vec<FunctionResult<A, P>>>,
        shared_storage: SharedStorage,
        duration: Arc<Mutex<Duration>>,
        new_txs: Arc<Mutex<Vec<Transaction<A, P>>>>
    ) -> JoinHandle<anyhow::Result<()>> {
        std::thread::spawn(move || {

            let pinned = core_affinity::set_for_current(CoreId{ id: executor_index });
            if !pinned {
                return Err(anyhow!("Unable to pin to executor core to CPU #{}", executor_index));
            }

            let mut core = Self{
                executor_index,
                shared_storage,
                output,
                duration,
                new_txs
            };

            while let Ok(mut received) = input.recv() {
                let a = Instant::now();
                core.execute(received);
                *core.duration.lock().unwrap() += a.elapsed();
            }

            Ok(())
        })
    }

    fn execute(&mut self, mut backlog: ExecutorInput<A, P>) {
        // VmUtils::timestamp(format!("Executor {} starts executing", self.executor_index).as_str());

        // let mut results = Vec::with_capacity(backlog.indices_range.len());
        let mut results = vec!();
        let mut new_txs = self.new_txs.lock().unwrap();

        for index in backlog.indices_range {
            let tx_index = backlog.scheduler_output.scheduled[index];
            let tx = backlog.scheduler_output.transactions.get(tx_index).unwrap();
            // let function = self.functions.get(tx.function as usize).unwrap();
            let function = tx.function;
            unsafe {
                match function.execute(tx.clone(), self.shared_storage.clone()) {
                    Another(generated_tx) => new_txs.push(generated_tx),
                    res => results.push(res),
                }
            }
        }

        if let Err(e) = self.output.send(results) {
            panic!("Failed to send transaction results: {:?}", e.into_inner());
        }

        VmUtils::timestamp(format!("Executor {} done executing", self.executor_index).as_str());
    }
}

// pub struct MixedCore<const A: usize, const P: usize> {
//     scheduler: SchedulingCore<A, P>,
//     executor: ExecutionCore<A, P>,
// }
//
// impl<const A: usize, const P: usize> MixedCore<A, P> {
//     pub fn spawn(input: Receiver<Job<A, P>>) {
//         std::thread::spawn(move || {
//
//             let mut core: MixedCore<A, P> = todo!();
//             let mut scheduling_duration = Duration::from_secs(0);
//             let mut execution_duration = Duration::from_secs(0);
//
//             while let Ok(mut job) = input.recv() {
//                 let a = Instant::now();
//                 match job {
//                     Job::Schedule(backlog) => {
//                         core.scheduler.schedule(backlog);
//                         scheduling_duration += a.elapsed();
//                     },
//                     Job::Execute(backlog) => {
//                         core.executor.execute(backlog);
//                         execution_duration += a.elapsed();
//                     }
//                 }
//             }
//
//             (scheduling_duration, execution_duration)
//         });
//     }
// }

pub struct VmResult<const A: usize, const P: usize> {
    pub results: Vec<FunctionResult<A, P>>,
    // pub scheduler_wait_duration: Option<Duration>,
    pub scheduler_msg_allocation: Option<Duration>,
    pub scheduler_msg_sending: Option<Duration>,
    pub scheduling_duration: Duration,

    // pub executor_wait_duration: Option<Duration>,
    pub executor_msg_allocation: Option<Duration>,
    pub executor_msg_sending: Option<Duration>,
    pub execution_duration: Duration,

    pub coordinator_wait_duration: Option<Duration>,
}
impl<const A: usize, const P: usize> VmResult<A, P> {
    pub fn new(results: Vec<FunctionResult<A, P>>, scheduling_duration: Option<Duration>, execution_duration: Option<Duration>) -> Self {
        let zero = Duration::from_secs(0);

        Self {
            results,
            scheduling_duration: scheduling_duration.unwrap_or(zero),
            execution_duration: execution_duration.unwrap_or(zero),
            scheduler_msg_sending: None,
            scheduler_msg_allocation: None,
            executor_msg_sending: None,
            executor_msg_allocation: None,
            coordinator_wait_duration: None,
        }
    }
}

//region coordinator ===============================================================================
pub struct Coordinator<const A: usize, const P: usize> {
    nb_schedulers: usize,
    to_schedulers: Vec<CrossSender<SchedulerInput<A, P>>>,
    from_schedulers: Vec<Receiver<SchedulerOutput<A, P>>>,
    scheduler_handles: Vec<JoinHandle<anyhow::Result<()>>>,
    local_scheduler: Option<SchedulingCore<A, P>>,
    outstanding_backlogs: Vec<usize>,
    scheduler_durations: Vec<Arc<Mutex<Duration>>>,

    nb_executors: usize,
    to_executors: Vec<CrossSender<ExecutorInput<A, P>>>,
    from_executors: Vec<Receiver<Vec<FunctionResult<A, P>>>>,
    executor_handles: Vec<JoinHandle<anyhow::Result<()>>>,
    local_executor: Option<(ExecutionCore<A, P>, Receiver<Vec<Transaction<A, P>>>)>,
    executor_durations: Vec<Arc<Mutex<Duration>>>,
    executor_new_txs: Vec<Arc<Mutex<Vec<Transaction<A, P>>>>>,

    pub storage: VmStorage,
}
impl<const A: usize, const P: usize> Coordinator<A, P> {
    pub fn new(batch_size: usize, storage_size: usize, nb_schedulers: usize, nb_executors: usize) -> Self {

        let pinned = core_affinity::set_for_current(CoreId{ id: nb_schedulers + nb_executors });
        if !pinned {
            panic!("Unable to pin to executor core to CPU #{}", nb_schedulers + nb_executors);
        }

        let storage = VmStorage::new(storage_size);

        let mut scheduler_handles = Vec::with_capacity(nb_schedulers);
        let mut to_schedulers = Vec::with_capacity(nb_schedulers);
        let mut from_schedulers = Vec::with_capacity(nb_schedulers);
        let mut scheduler_durations = Vec::with_capacity(nb_schedulers);

        // TODO Create local_scheduler?
        for scheduler_index in 0..nb_schedulers {
            let (send_work, receive_work) = unbounded();
            let (send_schedule, receive_schedule) = unbounded();
            let duration = Arc::new(Mutex::new(Duration::from_secs(0)));

            to_schedulers.push(send_work);
            from_schedulers.push(receive_schedule);
            scheduler_durations.push(duration.clone());

            scheduler_handles.push(SchedulingCore::spawn(
                scheduler_index,
                batch_size/nb_schedulers,
                receive_work,
                send_schedule,
                duration
            ));
        }

        let mut executor_handles = Vec::with_capacity(nb_executors);
        let mut to_executors = Vec::with_capacity(nb_executors);
        let mut from_executors = Vec::with_capacity(nb_executors);
        let mut executor_durations = Vec::with_capacity(nb_executors);
        let mut executor_new_txs = Vec::with_capacity(nb_executors);

        // TODO Create local_executor?
        for executor_index in 0..nb_executors {
            let (send_work, receive_work) = unbounded();
            let (send_results, receive_results) = unbounded();
            let duration = Arc::new(Mutex::new(Duration::from_secs(0)));
            let new_txs = Arc::new(Mutex::new(Vec::with_capacity(8192)));

            to_executors.push(send_work);
            from_executors.push(receive_results);
            executor_durations.push(duration.clone());
            executor_new_txs.push(new_txs.clone());

            executor_handles.push(ExecutionCore::spawn(
                nb_schedulers + executor_index,
                receive_work,
                send_results,
                storage.get_shared(),
                duration,
                new_txs
            ));
        }

        Self {
            nb_schedulers,
            to_schedulers,
            from_schedulers,
            scheduler_handles,
            local_scheduler: None,
            outstanding_backlogs: vec![0; nb_schedulers],
            scheduler_durations,
            nb_executors,
            to_executors,
            from_executors,
            executor_handles,
            local_executor: None,
            executor_durations,
            executor_new_txs,
            storage,
        }
    }

    pub fn execute(&mut self, mut batch: Vec<Transaction<A, P>>, immediate: bool) -> anyhow::Result<VmResult<A, P>> {

        /* Immediate Version (maybe good when there are lots of conflicts because the batch is broken down faster)
        *   Create backlog with content of batch
        *   Split backlog among S schedulers
        *
        *   Loop until executed batch_len tx:
        *       For scheduler 1 to S:
        *           Wait for schedule i (if empty skip)
        *           Split schedule among W executors
        *           Execute your own chunk
        *           Receive result from other executors
        *           Split new tx among S schedulers
        */
        /* Collect Version (maybe good when there is few conflicts as it will maximize parallelism)
        *   Create backlog with content of batch
        *   Split backlog among S schedulers
        *
        *   Loop until executed batch_len tx: (<=> backlog is empty)
        *       For scheduler 1 to S:
        *           Wait for schedule i (if empty skip)
        *           Split schedule among W executors
        *           Execute your own chunk
        *           Receive result from other executors (put them in backlog)
        *       Split backlog among S schedulers
        */

        let mut nb_executed = 0;
        let nb_to_execute = batch.len();
        // let mut results = vec![FunctionResult::Error; nb_to_execute];
        let mut results = Vec::with_capacity(nb_to_execute);

        // Create backlog with content of batch
        let mut backlog = batch;

        // let mut __iterations = Vec::with_capacity(65536);
        // let mut __coordinator_wait_duration = Duration::from_secs(0);
        // let mut __execution_rounds = 0;
        //
        // let mut __scheduler_msg_allocation = Duration::from_secs(0);
        // let mut __scheduler_msg_sending = Duration::from_secs(0);
        let mut __executor_msg_allocation = Duration::from_secs(0);
        let mut __executor_msg_sending = Duration::from_secs(0);

        while nb_executed < nb_to_execute {
            // let __tmp = Instant::now();
            // let mut __exec = Duration::from_secs(0);
            // let mut __wait = Duration::from_secs(0);
            // let mut __sending = Duration::from_secs(0);
            VmUtils::timestamp(format!("Coordinator: Start of loop (executed {} / {})", nb_executed, nb_to_execute).as_str());
            // For scheduler 1 to S: ---------------------------------------------------------------
            for (scheduler_index, scheduler) in self.from_schedulers.iter().enumerate() {

                if !backlog.is_empty() && (immediate || scheduler_index == 0) {
                    VmUtils::timestamp("Coordinator sending backlog to schedulers ==============");
                    for index in 0..self.nb_schedulers {
                        self.outstanding_backlogs[index] += 1;
                    }
                    // Split backlog among S schedulers ------------------------------------------------
                    let backlog_size = backlog.len();
                    let chunk_size = (backlog_size / self.to_schedulers.len()) + 1;
                    let transactions = Arc::new(backlog);
                    for (index, recipient) in self.to_schedulers.iter().enumerate() {
                        let range_start = index * chunk_size;
                        let range_end = min(range_start + chunk_size, backlog_size);
                        // let range_end = min((index + 1) * chunk_size, backlog_size);
                        let input = SchedulerInput{
                            transactions: transactions.clone(),
                            range: range_start..range_end,
                        };
                        recipient.send(input).expect(format!("Failed to send transactions to scheduler {}", index).as_str());
                    }

                    backlog = Vec::with_capacity(backlog_size);
                    // let (_alloc, _sending) = VmUtils::split::<A, P>(&mut backlog, &self.to_schedulers, "scheduler");
                    // __scheduler_msg_allocation += _alloc;
                    // __scheduler_msg_sending += _sending;
                    // assert!(backlog.is_empty());
                }

                if self.outstanding_backlogs[scheduler_index] == 0 {
                    VmUtils::timestamp(format!("scheduler {} doesn't have any outstanding backlogs, skipping", scheduler_index).as_str());
                    continue;
                }

                VmUtils::timestamp(format!("Waiting for scheduler {} ({} outstanding backlogs)",
                                           scheduler_index, self.outstanding_backlogs[scheduler_index]).as_str());
                // let __wait_start = Instant::now();
                if let Ok(mut scheduler_output) = scheduler.recv() {
                    // println!("Schedule from {}: {:?}", scheduler_index, schedule);
                    // __coordinator_wait_duration += __wait_start.elapsed();
                    // Wait for schedule i (if empty skip) -----------------------------------------
                    if scheduler_output.scheduled.is_empty() {
                        // println!("\tReceived empty schedule, scheduler completed one backlog");
                        self.outstanding_backlogs[scheduler_index] -= 1;
                        continue;
                    }

                    // Split schedule among W executors --------------------------------------------
                    VmUtils::timestamp("Coordinator sending backlog to executors");
                    let schedule_size = scheduler_output.scheduled.len();
                    let chunk_size = (schedule_size / self.to_executors.len()) + 1;
                    for (index, recipient) in self.to_executors.iter().enumerate() {
                        let range_start = index * chunk_size;
                        let range_end = min(range_start + chunk_size, schedule_size);
                        // let range_end = min((index + 1) * chunk_size, backlog_size);
                        let input = ExecutorInput{
                            scheduler_output: scheduler_output.clone(),
                            indices_range: range_start..range_end,
                        };
                        recipient.send(input).expect(format!("Failed to send transactions to scheduler {}", index).as_str());
                    }

                    // TODO have executors send the results to a separate mpsc channel and only send new txs to the coordinator
                    // Receive result from other executors -----------------------------------------
                    for (executor_index, executor) in self.from_executors.iter().enumerate() {
                        // eprintln!("Coordinator: Waiting for results from executor {}", executor_index);
                        if let Ok(mut res) = executor.recv() {
                            // eprintln!("\tGot result ({:?} results)", results_and_tx.len());
                            nb_executed += res.len();
                            results.append(&mut res);

                            let mut new_txs = self.executor_new_txs.get(executor_index).unwrap().lock().unwrap();
                            backlog.append(&mut *new_txs);
                            // Done with this executor result (could reuse vec)
                        } else {
                            panic!("Failed to receive executor result")
                        }
                        // Done with this executor
                    }

                    // __execution_rounds += 1;
                    // Done with this schedule (could reuse vec)
                    // assert!(scheduler_output.is_empty());
                } else {
                    panic!("Failed to receive schedule")
                }
                // Done with this scheduler
            }
            // Done one round robin
            // __iterations.push(__tmp.elapsed());
        }
        // println!(
        //     "\t{} iterations, mean iteration time: {}, total_time = {:?}",
        //      __iterations.len(),
        //      mean_ci_str(&__iterations),
        //     __iterations.iter().sum::<Duration>()
        // );

        // The whole batch was executed to completion, collect latency metrics
        let mut scheduling_duration = Duration::from_secs(0);
        for (index, mutex) in self.scheduler_durations.iter().enumerate() {
            let mut duration = mutex.lock().unwrap();
            // println!("Scheduling took {:?} for scheduler {}", *duration, index);
            scheduling_duration = max(scheduling_duration, *duration);
            *duration = Duration::from_secs(0);
        }
        let mut execution_duration = Duration::from_secs(0);
        for (index, mutex) in self.executor_durations.iter().enumerate() {
            let mut duration = mutex.lock().unwrap();
            // println!("Execution took {:?} for executor {}", *duration, index);
            execution_duration = max(execution_duration, *duration);
            *duration = Duration::from_secs(0);
        }

        // VmUtils::nb_rounds(__execution_rounds);
        let mut res = VmResult::new(results, Some(scheduling_duration), Some(execution_duration));
        // res.coordinator_wait_duration = Some(__coordinator_wait_duration);
        // res.scheduler_msg_allocation = Some(__scheduler_msg_allocation);
        // res.scheduler_msg_sending = Some(__scheduler_msg_sending);
        // res.executor_msg_allocation = Some(__executor_msg_allocation);
        // res.executor_msg_sending = Some(__executor_msg_sending);

        Ok(res)
    }

    pub fn execute_immediate(&mut self, mut batch: Vec<Transaction<A, P>>) -> anyhow::Result<VmResult<A, P>> {

        /* Immediate Version (maybe good when there are lots of conflicts because the batch is broken down faster)
        *   Create backlog with content of batch
        *   Split backlog among S schedulers
        *
        *   Loop until executed batch_len tx:
        *       For scheduler 1 to S:
        *           Wait for schedule i (if empty skip)
        *           Split schedule among W executors
        *           Execute your own chunk
        *           Receive result from other executors
        *           Split new tx among S schedulers
        */

        let mut nb_executed = 0;
        let nb_to_execute = batch.len();
        // let mut results = vec![FunctionResult::Error; nb_to_execute];
        let mut results = Vec::with_capacity(nb_to_execute);

        let mut __executor_msg_allocation = Duration::from_secs(0);
        let mut __executor_msg_sending = Duration::from_secs(0);

        VmUtils::timestamp("Coordinator sending backlog to schedulers ==============");
        for index in 0..self.nb_schedulers {
            self.outstanding_backlogs[index] += 1;
        }
        // Split backlog among S schedulers ------------------------------------------------
        let batch_size = batch.len();
        let chunk_size = (batch_size / self.to_schedulers.len()) + 1;
        let transactions = Arc::new(batch);
        for (index, recipient) in self.to_schedulers.iter().enumerate() {
            let range_start = index * chunk_size;
            let range_end = min(range_start + chunk_size, batch_size);
            // let range_end = min((index + 1) * chunk_size, backlog_size);
            let input = SchedulerInput{
                transactions: transactions.clone(),
                range: range_start..range_end,
            };
            recipient.send(input).expect(format!("Failed to send transactions to scheduler {}", index).as_str());
        }

        while nb_executed < nb_to_execute {
            VmUtils::timestamp(format!("Coordinator: Start of loop (executed {} / {})", nb_executed, nb_to_execute).as_str());
            // For scheduler 1 to S: ---------------------------------------------------------------
            for (scheduler_index, scheduler) in self.from_schedulers.iter().enumerate() {

                if self.outstanding_backlogs[scheduler_index] == 0 {
                    VmUtils::timestamp(format!("scheduler {} doesn't have any outstanding backlogs, skipping", scheduler_index).as_str());
                    continue;
                }

                VmUtils::timestamp(format!("Waiting for scheduler {} ({} outstanding backlogs)",
                                           scheduler_index, self.outstanding_backlogs[scheduler_index]).as_str());
                // let __wait_start = Instant::now();
                if let Ok(mut scheduler_output) = scheduler.recv() {
                    // println!("Schedule from {}: {:?}", scheduler_index, schedule);
                    // Wait for schedule i (if empty skip) -----------------------------------------
                    if scheduler_output.scheduled.is_empty() {
                        // println!("\tReceived empty schedule, scheduler completed one backlog");
                        self.outstanding_backlogs[scheduler_index] -= 1;
                        continue;
                    }

                    // Split schedule among W executors --------------------------------------------
                    VmUtils::timestamp("Coordinator sending backlog to executors");
                    let schedule_size = scheduler_output.scheduled.len();
                    let chunk_size = (schedule_size / self.to_executors.len()) + 1;
                    for (index, recipient) in self.to_executors.iter().enumerate() {
                        let range_start = index * chunk_size;
                        let range_end = min(range_start + chunk_size, schedule_size);
                        // let range_end = min((index + 1) * chunk_size, backlog_size);
                        let input = ExecutorInput{
                            scheduler_output: scheduler_output.clone(),
                            indices_range: range_start..range_end,
                        };
                        recipient.send(input).expect(format!("Failed to send transactions to scheduler {}", index).as_str());
                    }

                    // TODO have executors send the results to a separate mpsc channel and only send new txs to the coordinator
                    // Receive result from other executors -----------------------------------------
                    for (executor_index, executor) in self.from_executors.iter().enumerate() {
                        // eprintln!("Coordinator: Waiting for results from executor {}", executor_index);
                        if let Ok(mut res) = executor.recv() {
                            // eprintln!("\tGot result ({:?} results)", results_and_tx.len());
                            nb_executed += res.len();
                            results.append(&mut res);

                            let mut new_txs = self.executor_new_txs.get(executor_index).unwrap().lock().unwrap();
                            // backlog.append(&mut *new_txs);

                            if !new_txs.is_empty() {
                                VmUtils::timestamp("Coordinator sending backlog to schedulers ==============");
                                for index in 0..self.nb_schedulers {
                                    self.outstanding_backlogs[index] += 1;
                                }
                                // Split backlog among S schedulers ------------------------------------------------
                                let backlog_size = new_txs.len();
                                let chunk_size = (backlog_size / self.to_schedulers.len()) + 1;
                                let transactions = Arc::new(new_txs.clone());
                                for (index, recipient) in self.to_schedulers.iter().enumerate() {
                                    let range_start = index * chunk_size;
                                    let range_end = min(range_start + chunk_size, backlog_size);
                                    // let range_end = min((index + 1) * chunk_size, backlog_size);
                                    let input = SchedulerInput{
                                        transactions: transactions.clone(),
                                        range: range_start..range_end,
                                    };
                                    recipient.send(input).expect(format!("Failed to send transactions to scheduler {}", index).as_str());
                                }

                                new_txs.truncate(0);
                            }
                        } else {
                            panic!("Failed to receive executor result")
                        }
                        // Done with this executor
                    }

                    // __execution_rounds += 1;
                    // Done with this schedule (could reuse vec)
                    // assert!(scheduler_output.is_empty());
                } else {
                    panic!("Failed to receive schedule")
                }
                // Done with this scheduler
            }
            // Done one round robin
            // __iterations.push(__tmp.elapsed());
        }
        // println!(
        //     "\t{} iterations, mean iteration time: {}, total_time = {:?}",
        //      __iterations.len(),
        //      mean_ci_str(&__iterations),
        //     __iterations.iter().sum::<Duration>()
        // );

        // The whole batch was executed to completion, collect latency metrics
        let mut scheduling_duration = Duration::from_secs(0);
        for (index, mutex) in self.scheduler_durations.iter().enumerate() {
            let mut duration = mutex.lock().unwrap();
            // println!("Scheduling took {:?} for scheduler {}", *duration, index);
            scheduling_duration = max(scheduling_duration, *duration);
            *duration = Duration::from_secs(0);
        }
        let mut execution_duration = Duration::from_secs(0);
        for (index, mutex) in self.executor_durations.iter().enumerate() {
            let mut duration = mutex.lock().unwrap();
            // println!("Execution took {:?} for executor {}", *duration, index);
            execution_duration = max(execution_duration, *duration);
            *duration = Duration::from_secs(0);
        }

        // VmUtils::nb_rounds(__execution_rounds);
        let mut res = VmResult::new(results, Some(scheduling_duration), Some(execution_duration));
        // res.coordinator_wait_duration = Some(__coordinator_wait_duration);
        // res.scheduler_msg_allocation = Some(__scheduler_msg_allocation);
        // res.scheduler_msg_sending = Some(__scheduler_msg_sending);
        // res.executor_msg_allocation = Some(__executor_msg_allocation);
        // res.executor_msg_sending = Some(__executor_msg_sending);

        Ok(res)
    }

    pub fn init_storage(&mut self, init: Box<dyn Fn(&mut Vec<Word>)>) {
        init(&mut self.storage.content)
    }

    pub fn terminate(&mut self) -> (Vec<Duration>, Vec<Duration>) {
        // Drop the channels so the cores know that they can exit
        for scheduler in self.to_schedulers.drain(..) {
            drop(scheduler);
        }
        for executor in self.to_executors.drain(..) {
            drop(executor);
        }

        for scheduler in self.scheduler_handles.drain(..) {
            scheduler.join().unwrap().unwrap();
        }
        for executor in self.executor_handles.drain(..) {
            executor.join().unwrap().unwrap();
        }

        (vec!(), vec!())
    }
}
//endregion coordinator

#[derive(Clone, Debug)]
enum Signal {
    Ready,

    StartReadOnly,
    Start(usize),   // which schedule to execute
    Commit,
    Done
}
pub struct CoordinatorMixed<const A: usize, const P: usize> {
    nb_schedulers: usize,
    scheduler_handles: Vec<TokioHandle<anyhow::Result<()>>>,
    scheduler_inputs: Vec<TokioSender<Schedule<A, P>>>,

    nb_executors: usize,
    executor_handles: Vec<TokioHandle<anyhow::Result<()>>>,
    executor_outputs: Option<UnboundedReceiver<(usize, FunctionResult<A, P>)>>,

    send_terminate_signal: tokio::sync::broadcast::Sender<()>,
    storage: VmStorage,
}

#[derive(Debug)]
pub struct Schedule<const A: usize, const P: usize> {
    pub transactions: Vec<Transaction<A, P>>,
    pub read_only: Vec<usize>,
    pub exclusive: Vec<usize>,
    pub assignments: Vec<Vec<usize>>,
    pub nb_assigned_tx: usize,
    pub postponed: Vec<usize>,
}
impl<const A: usize, const P: usize> Schedule<A, P> {
    pub fn new(chunk_size: usize, nb_executors: usize) -> Self {
        Self::with_transactions(Vec::with_capacity(chunk_size), nb_executors)
    }

    pub fn with_transactions(transactions: Vec<Transaction<A, P>>, nb_executors: usize) -> Self {
        let chunk_size = transactions.len();
        Self {
            transactions,
            read_only: Vec::with_capacity(chunk_size),
            exclusive: Vec::with_capacity(chunk_size),
            assignments: vec![Vec::with_capacity(chunk_size); nb_executors],
            nb_assigned_tx: 0,
            postponed: Vec::with_capacity(chunk_size),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.transactions.is_empty()
    }

    pub fn clear(&mut self) {
        self.transactions.truncate(0);
        self.read_only.truncate(0);
        self.exclusive.truncate(0);
        self.assignments.iter_mut().for_each(|assignment| assignment.truncate(0));
        self.nb_assigned_tx = 0;
        self.postponed.truncate(0);
    }
}

impl<const A: usize, const P: usize> CoordinatorMixed<A, P> {
    pub fn new(batch_size: usize, storage_size: usize, nb_schedulers: usize, nb_executors: usize) -> Self {

        let storage = VmStorage::new(storage_size);

        let (send_terminate_signal, _receive_terminate_signal) = tokio::sync::broadcast::channel::<()>(1);

        // TODO For now, assume nb_schedulers == nb_executors
        assert_eq!(nb_schedulers, nb_executors);

        // Used to send the input from vm.execute
        let mut initial_scheduler_input = Vec::with_capacity(nb_schedulers);
        // Used to send results back to vm.execute
        let mut executor_outputs = tokio::sync::mpsc::unbounded_channel::<(usize, FunctionResult<A, P>)>();

        // Scheduler stuff -------------------------------------------------------------------------
        // scheduler[i].input = (send /*clone*/, receive);
        let mut scheduler_inputs = Vec::with_capacity(nb_schedulers);

        // scheduler[i].output = (send /*subscribe*/, receive);
        let mut scheduler_outputs = Vec::with_capacity(nb_schedulers);

        // Coordinator stuff -----------------------------------------------------------------------
        // coordinator.start_signal = (send /*subscribe*/, receive);
        let mut coordinator_start_signal = tokio::sync::broadcast::channel::<Signal>(1);

        // coordinator.ready_signal = (send /*clone*/, receive);
        let (send_ready_signal, mut receive_ready_signal) = tokio::sync::mpsc::channel::<Signal>(nb_executors);

        // Executor stuff --------------------------------------------------------------------------
        let mut schedulers_send_empty_struct = Vec::with_capacity(nb_executors);
        let mut executors_receive_empty_struct = Vec::with_capacity(nb_executors);

        for i in 0..nb_schedulers {
            let scheduler_i_input = tokio::sync::mpsc::channel::<Schedule<A, P>>(nb_executors);
            let scheduler_i_output = tokio::sync::broadcast::channel::<Arc<Schedule<A, P>>>(1);
            let (send_empty_struct, receive_empty_struct) = tokio::sync::mpsc::channel(1);

            initial_scheduler_input.push(scheduler_i_input.0.clone());
            schedulers_send_empty_struct.push(send_empty_struct);
            executors_receive_empty_struct.push(receive_empty_struct);

            scheduler_inputs.push(scheduler_i_input);
            scheduler_outputs.push(scheduler_i_output);
        }

        let mut coordinator = vec![receive_ready_signal];

        let executor_handles = (0..nb_executors).map(|i| {
            let mut receive_schedule = scheduler_outputs.iter().map(|(sender, _)| {
                sender.subscribe()
            }).collect_vec();

            let shared_storage = storage.get_shared();

            let mut receive_terminate_signal = send_terminate_signal.subscribe();

            let coordinator_send_start_signal = coordinator_start_signal.0.clone();
            let mut coordinator_receive_ready_signal = coordinator.pop();

            let mut receive_start_signal = coordinator_start_signal.0.subscribe();
            let send_ready_signal = send_ready_signal.clone();

            // Receive empty struct to write new transactions into
            let mut receive_reserved = executors_receive_empty_struct.pop().unwrap();

            // TODO For now only send output to a single scheduler => (nb_executor <= nb_schedulers)
            let send_new_txs = scheduler_inputs[i].0.clone();

            // TODO Change into a vec of either results or new_txs so new_txs can be reused for the next vm.execute?
            let send_results = executor_outputs.0.clone();
            let mut results: Vec<(usize, FunctionResult<A, P>)> = Vec::with_capacity(batch_size);

            let mut schedules = Vec::with_capacity(nb_schedulers);
            let mut non_empty_schedules: Vec<Signal> = Vec::with_capacity(nb_schedulers + 1);

            if let Some(mut receive_ready_signals) = coordinator_receive_ready_signal {
                // Spawn Coordinator
                tokio::spawn(async move {
                    // Executor 0 (Coordinator)
                    let executor_index = i;
                    let send_start_signal = coordinator_send_start_signal;

                    // The coordinator doesn't receive start signals, it sends them
                    drop(receive_start_signal);
                    // The coordinator doesn't send ready signals, it receives them
                    drop(send_ready_signal);

                    'main_loop: loop {
                        let mut done = true;
                        let mut has_exclusive = false;
                        let mut has_read_only = false;
                        let mut committed = false;

                        // Receive all schedules -------------------------------------------------------
                        for (index, mut receiver) in receive_schedule.iter_mut().enumerate() {
                            let schedule = tokio::select! {
                                received = receiver.recv() => {
                                    received.map_err(|err| anyhow!("Failed to receive schedule from scheduler {}", index))?
                                },
                                terminate = receive_terminate_signal.recv() => {
                                    return terminate.map_err(|err| anyhow!("Failed to receive termination signal"));
                                },
                            };

                            // If any schedule is non empty we are not done
                            if !schedule.is_empty() {
                                done = false;
                                has_read_only = has_read_only || !schedule.read_only.is_empty();
                                has_exclusive = has_exclusive || !schedule.exclusive.is_empty();
                                if schedule.nb_assigned_tx != 0 {
                                    non_empty_schedules.push(Signal::Start(index));
                                }
                            }

                            /* TODO technically, could drop empty schedules here (just make sure the
                                other executors also drop them, otherwise the indices won't match)
                             */
                            schedules.push(schedule);
                        }

                        // Receive an empty schedule to write new transactions into
                        // TODO Rename this
                        let mut empty_output: Schedule<A, P> = receive_reserved.recv().await.ok_or(
                            anyhow!("Failed to receive empty struct from scheduler {}", i)
                        )?;

                        if has_exclusive {
                            // We received all schedules and we know all other executors are waiting
                            Execution::execute_exclusive(
                                &schedules,
                                &mut results,
                                &mut empty_output.transactions,
                                shared_storage
                            );
                        }

                        if has_read_only {
                            non_empty_schedules.push(Signal::StartReadOnly);
                        }

                        if !non_empty_schedules.is_empty() {
                            let last_signal = non_empty_schedules.pop().unwrap();

                            // We can start executing the different schedules --------------------------
                            for signal in non_empty_schedules.drain(..) {
                                // Send start signal for this schedule
                                send_start_signal.send(signal.clone())
                                    .context("Coordinator: Failed to broadcast start signal")?;

                                // Execute schedule
                                match signal {
                                    Signal::StartReadOnly => {
                                        Execution::execute_read_only(
                                            &schedules,
                                            executor_index,
                                            nb_executors,
                                            &mut results,
                                            &mut empty_output.transactions,
                                            shared_storage
                                        );
                                    },
                                    Signal::Start(schedule_index) => {
                                        Execution::execute_assigned(
                                            &schedules,
                                            executor_index,
                                            schedule_index,
                                            &mut results,
                                            &mut empty_output.transactions,
                                            shared_storage
                                        );
                                    }
                                    _ => return Err(anyhow!("Unexpected signal for coordinator"))
                                }

                                // Wait for ready signals (the coordinator does not send a ready signal)
                                let expected_nb_signals = nb_executors - 1;
                                let mut nb_signals_received = 0;

                                while nb_signals_received < expected_nb_signals {
                                    receive_ready_signals.recv().await.ok_or(
                                        anyhow!("Coordinator: Failed to receive ready signal")
                                    )?;
                                    nb_signals_received += 1;
                                }
                            }

                            // Send start signal for last schedule
                            send_start_signal.send(last_signal.clone())
                                .context("Coordinator: Failed to broadcast start signal")?;

                            // Send commit signal early so executors can commit immediately after executing
                            // The goal is to enable schedulers to start as soon as possible
                            send_start_signal.send(Signal::Commit)
                                .context("Coordinator: Failed to broadcast commit signal")?;
                            committed = true;

                            // Execute schedule
                            match last_signal {
                                Signal::StartReadOnly => {
                                    Execution::execute_read_only(
                                        &schedules,
                                        executor_index,
                                        nb_executors,
                                        &mut results,
                                        &mut empty_output.transactions,
                                        shared_storage
                                    );
                                },
                                Signal::Start(schedule_index) => {
                                    Execution::execute_assigned(
                                        &schedules,
                                        executor_index,
                                        schedule_index,
                                        &mut results,
                                        &mut empty_output.transactions,
                                        shared_storage
                                    );
                                },
                                _ => return Err(anyhow!("Unexpected signal for coordinator"))
                            }
                        };

                        // The last schedule has been executed, we can commit the results
                        // No need for synchronization now, there won't be any execution until after the
                        // schedulers are done ans synchronize with the coordinator
                        if done {
                            // No transaction was executed (exclusive, read_only and assignments are empty)
                            // Send Done signal to other executors
                            send_start_signal.send(Signal::Done)
                                .context("Coordinator: Failed to broadcast Done signal")?;

                            // Drop empty new_txs TODO send to vm (so it can be reused later)
                            drop(empty_output);
                        } else {
                            if !committed {
                                // Send commit signal to other executors
                                // This case should only happen when there were only exclusive transactions to execute
                                send_start_signal.send(Signal::Commit)
                                    .context("Coordinator: Failed to broadcast commit signal")?;
                            }

                            // Send new_txs to scheduler j
                            /* Want to avoid using send.await because this task still has work to do
                                and we don't want to be switched out.
                               Also, the channel should never be out of capacity because we are the
                               only tasks sending on it and we are woken up only after the receiver
                               receives the message
                             */
                            send_new_txs.try_send(empty_output).context("Failed to send new transactions to scheduler")?;
                            // send_new_txs.send(empty_output).await?;
                        }

                        // drop schedules (so they can be reused later)
                        for schedule in schedules.drain(..) {
                            drop(schedule);
                        }

                        // Send transaction results TODO Send them all at the end?
                        for (tx_index, res) in results.drain(..) {
                            send_results.send((tx_index, res))?;
                        }

                        continue 'main_loop;
                    }

                    anyhow::Ok(())
                })
            } else {
                // Spawn executor
                tokio::spawn(async move {
                    // Executor i
                    let executor_index = i;

                    // Executors can't send start signals, they only receive them
                    drop(coordinator_send_start_signal);

                    'main_loop: loop {
                        // Receive all schedules -------------------------------------------------------
                        for (index, mut receiver) in receive_schedule.iter_mut().enumerate() {
                            // TODO Add graceful termination
                            // let schedule = receiver.recv().await
                            //     .context(format!("Failed to receive schedule from scheduler {}", index))?;
                            let schedule = tokio::select! {
                                received = receiver.recv() => {
                                    received.map_err(|err| anyhow!("Failed to receive schedule from scheduler {}", index))?
                                },
                                terminate = receive_terminate_signal.recv() => {
                                    return terminate.map_err(|err| anyhow!("Failed to receive termination signal"));
                                },
                            };

                            /* TODO technically, could drop empty schedules here (just make sure the
                                coordinator also drop them, otherwise the indices won't match)
                             */
                            schedules.push(schedule);
                        }

                        // Receive an empty schedule to write new transactions into
                        // TODO Rename this
                        let mut empty_output = receive_reserved.recv().await.ok_or(
                            anyhow!("Failed to receive empty struct from scheduler {}", i)
                        )?;

                        'execution_loop: loop {
                            // Receive signal from coordinator
                            match receive_start_signal.recv().await {
                                Ok(Signal::StartReadOnly) => {
                                    Execution::execute_read_only(
                                        &schedules,
                                        executor_index,
                                        nb_executors,
                                        &mut results,
                                        &mut empty_output.transactions,
                                        shared_storage
                                    );
                                },
                                Ok(Signal::Start(schedule_index)) => {
                                    // Execute schedule
                                    Execution::execute_assigned(
                                        &schedules,
                                        executor_index,
                                        schedule_index,
                                        &mut results,
                                        &mut empty_output.transactions,
                                        shared_storage
                                    );
                                },
                                Ok(Signal::Commit) => {
                                    // Send new_txs to scheduler j, use try_send to avoid being unscheduled
                                    send_new_txs.try_send(empty_output).context("Failed to send new transactions to scheduler")?;
                                    break 'execution_loop
                                },
                                Ok(Signal::Done) => {
                                    // NB: Should only receive done during the first iteration
                                    // Drop empty new_txs TODO send to vm (so it can be reused later)
                                    drop(empty_output);
                                    break 'execution_loop
                                },
                                other => {
                                    return Err(anyhow!("Executor: Received an unexpected signal: {:?}", other))
                                }
                            };
                        }

                        // drop schedules (so they can be reused later)
                        for schedule in schedules.drain(..) {
                            drop(schedule);
                        }

                        // Send transaction results (if any) TODO Send them all at the end?
                        for (tx_index, res) in results.drain(..) {
                            send_results.send((tx_index, res))?;
                        }

                        continue 'main_loop;
                    }

                    anyhow::Ok(())
                })
            }
        }).collect_vec();

        let scheduler_handles = scheduler_inputs
            .into_iter()
            .zip(scheduler_outputs.into_iter())
            .enumerate()
            .map(|(i, (input, output))| {

                let mut receive_terminate_signal = send_terminate_signal.subscribe();

                let mut receive_input: tokio::sync::mpsc::Receiver<Schedule<A, P>> = input.1;
                let send_empty_struct = schedulers_send_empty_struct.pop().unwrap();
                let broadcast_output = output.0;

                let chunk_size = batch_size/nb_schedulers;

                let mut previous = Arc::new(Schedule::new(chunk_size, nb_executors));
                let mut current = Arc::new(Schedule::new(chunk_size, nb_executors));

                let mut address_map: HashMap<StaticAddress, Assignment, BuildHasherDefault<AHasher>> = HashMap::with_capacity_and_hasher(
                    2 * A * batch_size, BuildHasherDefault::default()
                );
                let mut new_reads: Vec<StaticAddress> = Vec::with_capacity(batch_size);
                let mut new_writes: Vec<StaticAddress> = Vec::with_capacity(batch_size);

                // Spawn scheduler
                tokio::spawn(async move {
                    // Scheduler i initialization
                    // 'receive: while let Some(mut to_schedule) = receive_input.recv().await {
                    'receive: loop {
                        let mut to_schedule = tokio::select! {
                            received = receive_input.recv() => {
                                received.ok_or(anyhow!("Failed to receive backlog from executor/vm"))?
                            },
                            terminate = receive_terminate_signal.recv() => {
                                return terminate.map_err(|err| anyhow!("Failed to receive termination signal"));
                            },
                        };
                        /*
                            to_schedule: Schedule, transactions to schedule (the rest has been reset)
                            current: Arc<Schedule> might still be referenced by some executor
                            previous: Arc<Schedule> not referenced by anyone
                            empty <- previous.take.clear()
                            previous = current
                            current = Arc::new(to_schedule);
                            broadcast current
                            send empty to e_i so it can write its results
                         */

                        // Add postponed transaction to the backlog
                        for index in current.postponed.iter() {
                            let postponed_tx = current.transactions[*index].clone();
                            to_schedule.transactions.push(postponed_tx);
                        }

                        // Schedule the transactions
                        Scheduling::schedule_chunk_assign(
                            &mut address_map,
                            &mut new_reads,
                            &mut new_writes,
                            &mut to_schedule,
                        );

                        // Previous is guaranteed to have been dropped by all executors
                        // -> can be reused to store transactions generated by executors
                        let mut empty = Arc::try_unwrap(previous)
                            .map_err(|err| anyhow!("Failed to take previous schedule out of Arc"))?;

                        // Prepare executor output, clear the schedule information
                        empty.clear();

                        // Keep reference to current so that it can be reused later
                        previous = current;
                        current = Arc::new(to_schedule);

                        // Send schedule to all executors
                        broadcast_output.send(current.clone())
                            .context("Failed to broadcast schedule")?;

                        // Send empty struct to executor i so that it has somewhere to write its new transaction
                        // Try to avoid send.await because we are going to wait soon. No point being
                        //  unscheduled then rescheduled by the runtime if we are immediately going to wait anyway
                        send_empty_struct.try_send(empty)
                            .context("Failed to send empty struct to executor")?;
                        // send_empty_struct.send(empty).await.
                        //     context("Failed to send empty struct to executor")?;
                    }

                    anyhow::Ok(())
                })
        }).collect_vec();

        Self {
            nb_schedulers,
            scheduler_handles,
            scheduler_inputs: initial_scheduler_input,

            nb_executors,
            executor_handles,
            executor_outputs: Some(executor_outputs.1),

            send_terminate_signal,
            storage,
        }
    }

    pub async fn execute(&mut self, mut batch: Vec<Transaction<A, P>>) -> anyhow::Result<VmResult<A, P>> {

        let batch_size = batch.len();
        let chunk_size = batch_size/self.nb_schedulers;

        batch.into_iter()
            .chunks(chunk_size).into_iter()
            .zip(self.scheduler_inputs.iter())
            .try_for_each(|(chunk, sender)| {
                let backlog = Schedule::with_transactions(chunk.collect_vec(), self.nb_executors);
                sender.try_send(backlog)
                    .context("Failed to send initial backlog")
        })?;

        if let Some(mut receive_results) = self.executor_outputs.take() {
            let handle = tokio::spawn(async move {
                let mut nb_executed = 0;
                let mut results = vec![FunctionResult::Error; batch_size];

                while nb_executed < batch_size {
                    match receive_results.recv().await {
                        Some((index, res)) => {
                            results[index] = res;
                            nb_executed += 1;
                        },
                        None => {
                            return Err(anyhow!("Failed to receive executor outputs"));
                        }
                    }
                }

                Ok((receive_results, results))
            });

            let (receiver, results) = handle.await.unwrap()?;

            // Put the receiver back for next execution
            self.executor_outputs = Some(receiver);

            // TODO Find a way to receive durations from schedulers and executors (... could do this in terminate...)
            let mut vm_result = VmResult::new(
                results,
                None,//Some(scheduling_duration),
                None,//Some(execution_duration),
            );
            Ok(vm_result)
        } else {
            Err(anyhow!("Unable to take channel receiver"))
        }
    }

    pub fn init_storage(&mut self, init: Box<dyn Fn(&mut Vec<Word>)>) {
        init(&mut self.storage.content)
    }

    pub async fn terminate(&mut self) -> (Vec<Duration>, Vec<Duration>) {

        self.send_terminate_signal.send(())
            .context("Failed to send termination signal").unwrap();

        // join the executors first, otherwise they might exit with an error since they are listening
        // on the schedulers channels
        for executor in self.executor_handles.drain(..) {
            executor.await
                .context("Failed to join executor").unwrap()
                .context("Executor returned with an error").unwrap();
        }

        for scheduler in self.scheduler_handles.drain(..) {
            scheduler.await
                .context("Failed to join scheduler").unwrap()
                .context("Scheduler returned with an error").unwrap();
        }

        (vec!(), vec!())
    }
}

//region VM Types ==================================================================================
#[derive(Debug, Serialize, Deserialize, Copy, Clone, PartialEq)]
pub enum VmType {
    A,
    BTokio,
    BStd,
    C,
    Sequential,
    ParallelCollect,
    ParallelImmediate,
    Immediate,
    Collect,
    Mixed
}

impl VmType {
    pub fn name(&self) -> String {
        match self {
            VmType::A => String::from("VmA"),
            VmType::BTokio => String::from("VmB_Tokio"),
            VmType::BStd => String::from("VmB_Std"),
            VmType::C => String::from("VmC"),
            VmType::Sequential => String::from("Sequential"),
            VmType::ParallelCollect => String::from("ParallelCollect"),
            VmType::ParallelImmediate => String::from("ParallelImmediate"),
            VmType::Immediate => String::from("Immediate"),
            VmType::Collect => String::from("Collect"),
            VmType::Mixed => String::from("Mixed"),
        }
    }

    pub fn new(&self) -> bool {
        match self {
            VmType::Sequential => true,
            VmType::ParallelCollect => true,
            VmType::ParallelImmediate => true,
            VmType::Immediate => true,
            VmType::Collect => true,
            VmType::Mixed => true,
            _ => false
        }
    }
}
//endregion

//region VM Factory ================================================================================
pub struct VmFactory;
impl VmFactory {
    pub fn from(p: &RunParameter) -> Box<dyn Executor> {
        match p.vm_type {
            VmType::A => Box::new(VMa::new(p.storage_size).unwrap()),
            VmType::BTokio => Box::new(VMb::<WorkerBTokio>::new(p. storage_size, p.nb_executors, p.batch_size).unwrap()),
            VmType::BStd => Box::new(VMb::<WorkerBStd>::new(p. storage_size, p.nb_executors, p.batch_size).unwrap()),
            VmType::C => Box::new(VMc::new(p. storage_size, p.nb_executors, p.batch_size).unwrap()),
            _ => todo!()
        }
    }
}
//endregion

//region VM storage ================================================================================
#[derive(Debug)]
pub struct VmStorage {
    pub content: Vec<Word>,
    pub shared: SharedStorage,
}

impl VmStorage {
    pub fn new(size: usize) -> Self {
        let mut content = vec![0 as Word; size];
        let ptr = content.as_mut_ptr();
        let shared = SharedStorage { ptr, size };

        return Self{ content, shared};
    }

    pub fn len(&self) -> usize {
        return self.content.len();
    }

    pub fn get(&self, index: usize) -> Word {
        return self.content[index];
    }

    pub fn set(&mut self, index: usize, value: Word) {
        self.content[index] = value;
    }

    pub fn get_shared(&self) -> SharedStorage {
        return self.shared;
    }

    pub fn set_storage(&mut self, value: Word) {
        self.content.fill(value);
    }

    pub fn total(&self) -> Word {
        self.content.iter().fold(0, |a, b| a + *b)
    }
}

#[derive(Copy, Clone, Debug)]
pub struct SharedStorage {
    pub ptr: *mut Word,
    pub size: usize,
}

unsafe impl Send for SharedStorage {}
unsafe impl Sync for SharedStorage {}

impl SharedStorage {
    pub fn get(&self, index: usize) -> Word {
        unsafe {
            *self.ptr.add(index)
        }
    }

    pub fn set(&mut self, index: usize, value: Word) {
        unsafe {
            *self.ptr.add(index) = value;
        }
    }

    pub fn get_mut(&mut self, index: usize) -> *mut Word {
        unsafe {
            self.ptr.add(index)
        }
    }

    pub fn len(&self) -> usize {
        self.size
    }

    pub fn to_vec(&self) -> Vec<Word> {
        unsafe {
            Vec::from_raw_parts(self.ptr, self.size, self.size)
        }
    }
}
//endregion

#[derive(Clone, Debug)]
pub struct AddressSet {
    pub inner: ThinSet<StaticAddress>
}
unsafe impl Send for AddressSet {}
// unsafe impl Sync for AddressSet {}

impl AddressSet {
    pub fn with_capacity_and_max(cap: usize, _max: StaticAddress) -> Self {
        let inner = ThinSet::with_capacity(cap);
        return Self { inner };
    }
    pub fn with_capacity(cap: usize) -> Self {
        let inner = ThinSet::with_capacity(cap);
        return Self { inner };
    }
    #[inline]
    pub fn contains(&self, el: StaticAddress) -> bool {
        self.inner.contains(&el)
    }
    #[inline]
    pub fn insert(&mut self, el: StaticAddress) -> bool {
        self.inner.insert(el)
    }
    #[inline]
    pub fn clear(&mut self) {
        self.inner.clear()
    }
}

pub const UNASSIGNED: AssignedWorker = 0;
pub const CONFLICTING: AssignedWorker = AssignedWorker::MAX-1;

pub fn assign_workers(
    nb_workers: usize,
    batch: &Jobs,
    address_to_worker: &mut Vec<AssignedWorker>,
    backlog: &mut Jobs
) -> Vec<AssignedWorker> {
    let mut tx_to_worker = vec![UNASSIGNED; batch.len()];
    let mut next_worker = 0 as AssignedWorker;
    let nb_workers = nb_workers as AssignedWorker;

    for (index, tx) in batch.iter().enumerate() {
        let from = tx.from as usize;
        let to = tx.to as usize;

        let worker_from = address_to_worker[from];
        let worker_to = address_to_worker[to];

        if worker_from == UNASSIGNED && worker_to == UNASSIGNED {
            let assigned = next_worker + 1;
            address_to_worker[from] = assigned;
            address_to_worker[to] = assigned;
            tx_to_worker[index] = assigned;
            next_worker = if next_worker == nb_workers - 1 {
                0
            } else {
                next_worker + 1
            };
        } else if worker_from == UNASSIGNED || worker_to == UNASSIGNED {
            let assigned = max(worker_from, worker_to);
            address_to_worker[from] = assigned;
            address_to_worker[to] = assigned;
            tx_to_worker[index] = assigned;

        } else if worker_from == worker_to {
            tx_to_worker[index] = worker_from;

        } else {
            backlog.push(tx.clone());
        }
    }

    return tx_to_worker;
}
