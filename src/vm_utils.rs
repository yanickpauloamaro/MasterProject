use std::cell::RefCell;
use std::cmp::max;
use std::collections::HashMap;
use std::fmt::format;
use std::hash::BuildHasherDefault;
use std::{cmp, mem};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
// use std::sync::mpsc::Receiver;
use std::time::{Instant, Duration, SystemTime, UNIX_EPOCH};

use ahash::AHasher;
use anyhow::anyhow;
use core_affinity::CoreId;
use crossbeam::channel::{Receiver, Sender, unbounded};
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use thincollections::thin_set::ThinSet;
// use tokio::time::Instant;

use crate::config::RunParameter;
use crate::contract::{AccessPattern, AccessType, AtomicFunction, FunctionResult, StaticAddress, Transaction};
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
        recipients: &Vec<Sender<Vec<Transaction<A, P>>>>,
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

    fn executor_pool<const A: usize, const P: usize>(
        to_executors: &Vec<Sender<Vec<Transaction<A, P>>>>,
        from_executors: &Vec<Receiver<Vec<FunctionResult<A, P>>>>,
        schedule: &mut Vec<Transaction<A, P>>,
        backlog: &mut Vec<Transaction<A, P>>,
        results: &mut Vec<FunctionResult<A, P>>,
        mut __executor_msg_allocation: &mut Duration,
        mut __executor_msg_sending: &mut Duration,
    ) -> usize {
        let mut nb_executed = 0;
        // eprintln!("Coordinator: Sending backlog to executors");
        // eprintln!("---------------------------");
        VmUtils::timestamp("Coordinator sending backlog to executors");

        let (alloc, sending) = VmUtils::split::<A, P>(schedule, to_executors, "executor");
        *__executor_msg_allocation += alloc;
        *__executor_msg_sending += sending;
        assert!(schedule.is_empty());

        // TODO Execute your own chunk ------------------------------------------------------

        // Receive result from other executors -----------------------------------------
        for (executor_index, executor) in from_executors.iter().enumerate() {
            // eprintln!("Coordinator: Waiting for results from executor {}", executor_index);
            if let Ok(mut results_and_tx) = executor.recv() {
                // eprintln!("\tGot result ({:?} results)", results_and_tx.len());
                for res in results_and_tx.drain(..) {
                    match res {
                        Another(tx) => backlog.push(tx),
                        res => {
                            nb_executed += 1;
                            results.push(res)
                        },
                    }
                }
                // Done with this executor result (could reuse vec)
            } else {
                panic!("Failed to receive executor result")
            }
            // Done with this executor
        }

        nb_executed
    }
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

    pub fn schedule_chunk_new<const A: usize, const P: usize>(
        mut chunk: &mut Vec<Transaction<A, P>>,
        mut scheduled: &mut Vec<Transaction<A, P>>,
        mut postponed: &mut Vec<Transaction<A, P>>,
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

        'backlog: for tx in chunk.drain(0..) {
            remainder += 1;
            // let base_case_start = Instant::now();
            let (possible_reads, possible_writes) = tx.accesses();
            let reads = possible_reads.as_ref().map_or([].as_slice(), |inside| inside.as_slice());
            let writes = possible_writes.as_ref().map_or([].as_slice(), |inside| inside.as_slice());

            // Tx without any memory accesses ------------------------------------------------------
            if reads.is_empty() && writes.is_empty() {
                scheduled.push(tx);
                continue 'backlog;
            }

            // Only reads are allowed --------------------------------------------------------------
            if read_locked {
                if writes.is_empty() { scheduled.push(tx); }
                else { postponed.push(tx); }
                continue 'backlog;
            }

            // All transactions with accesses will be postponed ------------------------------------
            if write_locked {
                postponed.push(tx);
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
                            postponed.push(tx);
                            // reads_duration += read_start.elapsed();
                            continue 'backlog;
                        }
                    },
                    AccessPattern::Range(from, to) => {
                        some_reads = true;
                        assert!(from < to);
                        'range: for addr in (*from)..(*to) {
                            if !can_read(addr, &mut address_map) {
                                postponed.push(tx);
                                // reads_duration += read_start.elapsed();
                                continue 'backlog;
                            }
                        }
                    },
                    AccessPattern::All => {
                        if some_writes {
                            postponed.push(tx);
                        } else {
                            scheduled.push(tx);
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
                            postponed.push(tx);
                            // writes_duration += writes_start.elapsed();
                            continue 'backlog;
                        }
                    },
                    AccessPattern::Range(from, to) => {
                        some_writes = true;
                        assert!(from < to);
                        'range: for addr in (*from)..(*to) {
                            if !can_write(addr, &mut address_map) {
                                postponed.push(tx);
                                // writes_duration += writes_start.elapsed();
                                continue 'backlog;
                            }
                        }
                    },
                    AccessPattern::All => {
                        if some_reads || some_writes {
                            postponed.push(tx);
                        } else {
                            scheduled.push(tx);
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
            scheduled.push(tx);
        }

        // let total = a.elapsed();
        // println!("Scheduled {:?}", scheduled);
        // println!("Scheduled: {}, postponed: {}, took {:?}", scheduled.len(), postponed.len(), total);
        // total, (init, base_case, reads, writes)
        // println!("\ttook {:.3?} \t({:?} µs, {:?} µs, {:?} µs, {:?} µs)", total, init_duration.as_micros(), base_case_duration.as_micros(), reads_duration.as_micros(), writes_duration.as_micros());
        // (scheduled, postponed)
    }
}

pub struct SchedulingCore<const A: usize, const P: usize> {
    scheduler_index: usize,
    address_map: RefCell<Map>,
    postponed: RefCell<Vec<Transaction<A, P>>>,
    output: Sender<Vec<Transaction<A, P>>>,
    duration: Arc<Mutex<Duration>>
}
impl<const A: usize, const P: usize> SchedulingCore<A, P> {
    pub fn spawn(
        scheduler_index: usize,
        chunk_size: usize,
        input: Receiver<Vec<Transaction<A, P>>>,
        output: Sender<Vec<Transaction<A, P>>>,
        duration: Arc<Mutex<Duration>>
    ) -> JoinHandle<anyhow::Result<()>> {
        std::thread::spawn(move || {

            let pinned = core_affinity::set_for_current(CoreId{ id: scheduler_index });
            if !pinned {
                return Err(anyhow!("Unable to pin to scheduler core to CPU #{}", scheduler_index));
            }

            let mut address_map = HashMap::with_capacity_and_hasher(A * chunk_size, BuildHasherDefault::default());
            let mut postponed = Vec::with_capacity(chunk_size);

            let mut core = Self{
                scheduler_index,
                address_map: RefCell::new(address_map),
                postponed: RefCell::new(postponed),
                output,
                duration
            };

            while let Ok(mut backlog) = input.recv() {

                let a = Instant::now();
                core.schedule(backlog);
                *core.duration.lock().unwrap() += a.elapsed();
            }

            Ok(())
        })
    }

    fn schedule(&mut self, mut backlog: Vec<Transaction<A, P>>) {
        // VmUtils::timestamp(format!("Scheduler {} starts scheduling", self.scheduler_index).as_str());
        while !backlog.is_empty() {
            // let c = Instant::now();
            let mut scheduled = Vec::with_capacity(backlog.len());

            Scheduling::schedule_chunk_new(
                &mut backlog,
                &mut scheduled,
                self.postponed.get_mut(),
                self.address_map.get_mut(),
                // c
            );
            if scheduled.is_empty() { panic!("Scheduler produced an empty schedule"); }
            // backlog is now empty

            // VmUtils::timestamp(format!("\tScheduler {} sends schedule", self.scheduler_index).as_str());
            if let Err(e) = self.output.send(scheduled) {
                panic!("Failed to send schedule: {:?}", e.into_inner());
            }

            mem::swap(&mut backlog, self.postponed.get_mut());
            self.address_map.get_mut().clear();
        }

        if let Err(e) = self.output.send(backlog) {
            panic!("Failed to send end: {:?}", e.into_inner());
        }

        VmUtils::timestamp(format!("Scheduler {} done scheduling", self.scheduler_index).as_str());
    }
}

pub struct ExecutionCore<const A: usize, const P: usize> {
    pub executor_index: usize,
    pub shared_storage: SharedStorage,
    pub output: Sender<Vec<FunctionResult<A, P>>>,
    pub duration: Arc<Mutex<Duration>>
}
impl<const A: usize, const P: usize> ExecutionCore<A, P> {

    pub fn spawn(
        executor_index: usize,
        input: Receiver<Vec<Transaction<A, P>>>,
        output: Sender<Vec<FunctionResult<A, P>>>,
        shared_storage: SharedStorage,
        duration: Arc<Mutex<Duration>>
    ) -> JoinHandle<anyhow::Result<()>> {
        std::thread::spawn(move || {

            let pinned = core_affinity::set_for_current(CoreId{ id: executor_index });
            if !pinned {
                return Err(anyhow!("Unable to pin to executor core to CPU #{}", executor_index));
            }

            let core = Self{
                executor_index,
                shared_storage,
                output,
                duration
            };

            while let Ok(mut backlog) = input.recv() {
                let a = Instant::now();
                core.execute(backlog);
                *core.duration.lock().unwrap() += a.elapsed();
            }

            Ok(())
        })
    }

    fn execute(&self, mut backlog: Vec<Transaction<A, P>>) {
        // VmUtils::timestamp(format!("Executor {} starts executing", self.executor_index).as_str());

        let mut results = Vec::with_capacity(backlog.len());
        for tx in backlog {
            // let function = self.functions.get(tx.function as usize).unwrap();
            let function = tx.function;
            unsafe {
                let tx_result = function.execute(tx.clone(), self.shared_storage.clone());
                results.push(tx_result);
            }
        }

        if let Err(e) = self.output.send(results) {
            panic!("Failed to send transaction results: {:?}", e.into_inner());
        }

        VmUtils::timestamp(format!("Executor {} done executing", self.executor_index).as_str());
    }
}

pub struct MixedCore<const A: usize, const P: usize> {
    scheduler: SchedulingCore<A, P>,
    executor: ExecutionCore<A, P>,
}

impl<const A: usize, const P: usize> MixedCore<A, P> {
    pub fn spawn(input: Receiver<Job<A, P>>) {
        std::thread::spawn(move || {

            let mut core: MixedCore<A, P> = todo!();
            let mut scheduling_duration = Duration::from_secs(0);
            let mut execution_duration = Duration::from_secs(0);

            while let Ok(mut job) = input.recv() {
                let a = Instant::now();
                match job {
                    Job::Schedule(backlog) => {
                        core.scheduler.schedule(backlog);
                        scheduling_duration += a.elapsed();
                    },
                    Job::Execute(backlog) => {
                        core.executor.execute(backlog);
                        execution_duration += a.elapsed();
                    }
                }
            }

            (scheduling_duration, execution_duration)
        });
    }
}

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

pub struct Coordinator<const A: usize, const P: usize> {
    nb_schedulers: usize,
    to_schedulers: Vec<Sender<Vec<Transaction<A, P>>>>,
    from_schedulers: Vec<Receiver<Vec<Transaction<A, P>>>>,
    scheduler_handles: Vec<JoinHandle<anyhow::Result<()>>>,
    local_scheduler: Option<SchedulingCore<A, P>>,
    outstanding_backlogs: Vec<usize>,
    scheduler_durations: Vec<Arc<Mutex<Duration>>>,

    nb_executors: usize,
    to_executors: Vec<Sender<Vec<Transaction<A, P>>>>,
    from_executors: Vec<Receiver<Vec<FunctionResult<A, P>>>>,
    executor_handles: Vec<JoinHandle<anyhow::Result<()>>>,
    local_executor: Option<(ExecutionCore<A, P>, Receiver<Vec<Transaction<A, P>>>)>,
    executor_durations: Vec<Arc<Mutex<Duration>>>,

    pub storage: VmStorage,
}
impl<const A: usize, const P: usize> Coordinator<A, P> {
    pub fn new(batch_size: usize, storage_size: usize, nb_schedulers: usize, nb_executors: usize) -> Self {

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
        let mut executor_durations = Vec::with_capacity(nb_schedulers);

        // TODO Create local_executor?
        for executor_index in 0..nb_executors {
            let (send_work, receive_work) = unbounded();
            let (send_results, receive_results) = unbounded();
            let duration = Arc::new(Mutex::new(Duration::from_secs(0)));

            to_executors.push(send_work);
            from_executors.push(receive_results);
            executor_durations.push(duration.clone());

            executor_handles.push(ExecutionCore::spawn(
                nb_schedulers + executor_index,
                receive_work,
                send_results,
                storage.get_shared(),
                duration
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
        let mut backlog = vec!();
        backlog.append(&mut batch);

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
                    let (_alloc, _sending) = VmUtils::split::<A, P>(&mut backlog, &self.to_schedulers, "scheduler");
                    // __scheduler_msg_allocation += _alloc;
                    // __scheduler_msg_sending += _sending;
                    assert!(backlog.is_empty());
                }

                if self.outstanding_backlogs[scheduler_index] == 0 {
                    VmUtils::timestamp(format!("scheduler {} doesn't have any outstanding backlogs, skipping", scheduler_index).as_str());
                    continue;
                }

                VmUtils::timestamp(format!("Waiting for scheduler {} ({} outstanding backlogs)",
                                           scheduler_index, self.outstanding_backlogs[scheduler_index]).as_str());
                // let __wait_start = Instant::now();
                if let Ok(mut schedule) = scheduler.recv() {
                    // println!("Schedule from {}: {:?}", scheduler_index, schedule);
                    // __coordinator_wait_duration += __wait_start.elapsed();
                    // Wait for schedule i (if empty skip) -----------------------------------------
                    if schedule.is_empty() {
                        // println!("\tReceived empty schedule, scheduler completed one backlog");
                        self.outstanding_backlogs[scheduler_index] -= 1;
                        continue;
                    }

                    // Split schedule among W executors --------------------------------------------
                    nb_executed += VmUtils::executor_pool::<A, P>(
                        &self.to_executors,
                        &self.from_executors,
                        &mut schedule,
                        &mut backlog,
                        &mut results,
                        &mut __executor_msg_allocation,
                        &mut __executor_msg_sending,
                    );
                    // __execution_rounds += 1;
                    // Done with this schedule (could reuse vec)
                    assert!(schedule.is_empty());
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
        for mutex in self.scheduler_durations.iter() {
            let mut duration = mutex.lock().unwrap();
            scheduling_duration = max(scheduling_duration, *duration);
            *duration = Duration::from_secs(0);
        }
        let mut execution_duration = Duration::from_secs(0);
        for mutex in self.executor_durations.iter() {
            let mut duration = mutex.lock().unwrap();
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

pub struct CoordinatorMixed<const A: usize, const P: usize> {
    scheduler: SchedulingCore<A, P>,
    executor: ExecutionCore<A, P>,
    storage: VmStorage,
}
impl<const A: usize, const P: usize> CoordinatorMixed<A, P> {
    pub fn new(batch_size: usize, storage_size: usize, nb_schedulers: usize, nb_executors: usize) -> Self {
        todo!()
    }

    pub fn execute(&mut self, mut batch: Vec<Transaction<A, P>>) -> anyhow::Result<VmResult<A, P>> {

        /* Mixed Version
        *   Create backlog with content of batch
        *
        *   Loop until executed batch_len tx: (<=> backlog is empty)
        *       Split backlog among N mixed_schedulers (send empty if not enough tx)
        *       Wait to receive schedules
        *       For schedule 1 to N:
        *           1) Sequentially execute Exclusive tx (self.executor)
        *           2) Split Read-only schedule among N mixed_executors
        *               Wait to receive results (add to backlog)
        *           3) Split Parallel schedule among N mixed_executors
        *               Wait to receive results (add to backlog)
        *   NB: If Coordinator is included in N, execute you own chunk after sending work to other cores
        */

        todo!()
    }

    pub fn init_storage(&mut self, init: Box<dyn Fn(&mut Vec<Word>)>) {
        init(&mut self.storage.content)
    }

    pub fn terminate(&mut self) -> (Vec<Duration>, Vec<Duration>) {
        todo!()
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
