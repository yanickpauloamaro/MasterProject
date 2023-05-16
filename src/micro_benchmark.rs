// use hwloc2::{Topology, TopologyObject, ObjectType, CpuBindFlags};
use ahash::AHasher;
use std::collections::HashMap;
use hashbrown::HashMap as BrownMap;
use std::fmt::Debug;
use std::hash::BuildHasherDefault;
use std::mem;
use anyhow::anyhow;
use core_affinity::CoreId;
use itertools::Itertools;
use nohash_hasher::{BuildNoHashHasher, NoHashHasher};
use rand::prelude::{SliceRandom, StdRng};
use rand::{RngCore, SeedableRng};
use tokio::time::{Duration, Instant};
use tabled::{Table, Tabled};
use thincollections::thin_map::ThinMap;
use crate::benchmark::WorkloadUtils;
use crate::contract::{AccessPattern, AccessType, AtomicFunction, SenderAddress, StaticAddress, Transaction};
use crate::key_value::KeyValueOperation;
use crate::parallel_vm::ParallelVmImmediate;
use crate::sequential_vm::SequentialVM;
use crate::utils::mean_ci;
use crate::vm_utils::{AddressSet, Assignment, Scheduling};
use crate::wip::Word;

//region NUMA latency ------------------------------------------------------------------------------
const KILO_OFFSET: usize = 10;
const MEGA_OFFSET: usize = 20;
const GIGA_OFFSET: usize = 30;
const KB: usize = 1 << KILO_OFFSET;
const MB: usize = 1 << MEGA_OFFSET;
const GB: usize = 1 << GIGA_OFFSET;

const SEED: u64 = 10;
const MILLION: usize = 1 * 1000 * 1000; // 1M operations per iteration

// =================================================================================================
// getconf -a | grep CACHE
// Laptop
const L1_SIZE: usize = 32 * KB;    // per core
const L2_SIZE: usize = 256 * KB;   // per core
const L3_SIZE: usize = 6 * MB;     // shared
const CACHE_LINE_SIZE: u8 = 64;

// // AWS
// let L1_SIZE = 48 * kilo;            // 48 KB (per core)
// let L2_SIZE = 1 * mega + mega/4;    // 1.25 MB (per core)
// let L3_SIZE = 54 * mega;            // 54 MB (shared)
// const CACHE_LINE_SIZE: usize = 64;
// =================================================================================================

//region Measurements ------------------------------------------------------------------------------
#[derive(Clone)]
struct Measurement {
    execution_core: usize,
    memory_core: usize,
    array_size_bytes: usize,
    mean_latency: Duration,
    ci_mean_latency: Duration,
    nb_iterations: usize,
    nb_op_per_iteration: usize,
    total_duration: Duration,
}

impl Measurement {
    fn new(execution_core: usize,
           memory_core: usize,
           array_size_bytes: usize,
           latencies: &Vec<Duration>,
           total_duration: Duration,
            nb_operations: usize
    ) -> Self {
        let (mean_latency, ci_mean_latency) = mean_ci(latencies);
        Self {
            execution_core,
            memory_core,
            array_size_bytes,
            mean_latency,
            ci_mean_latency,
            nb_iterations: latencies.len(),
            nb_op_per_iteration: nb_operations,
            total_duration,
        }
    }

    fn to_json(&self) -> String {
        let output = vec![
            "{".to_string(),
            format!("execution_core: {},", self.execution_core),
            format!("memory_core: {},", self.memory_core),
            format!("array_size_bytes: {},", self.array_size_bytes),
            format!("mean_latency_ns_per_op: {:.3},", (self.mean_latency.as_nanos() as f64) / (self.nb_op_per_iteration as f64)),
            format!("ci_mean_latency_ns_per_op: {:.3},", (self.ci_mean_latency.as_nanos() as f64) / (self.nb_op_per_iteration as f64)),
            format!("nb_iterations: {},", self.nb_iterations),
            format!("total_duration_ms: {},", self.total_duration.as_millis()),
            "},".to_string()
        ];

        output.join(" ")
    }
}

#[derive(Tabled, Clone)]
struct SizeLatencyMeasurement {
    array_size: String,
    mean: String,
    ci: String,
    nb_iterations: usize,
    total_duration: String
}

impl SizeLatencyMeasurement {
    fn from(measurement: &Measurement) -> Self {
        Self {
            array_size: adapt_unit(measurement.array_size_bytes),
            mean: format!("{:.3?}", measurement.mean_latency),
            ci: format!("{:.3?}", measurement.ci_mean_latency),
            nb_iterations: measurement.nb_iterations,
            total_duration: format!("{:.3?}", measurement.total_duration)
        }
    }
}

#[derive(Tabled, Clone)]
struct CoreLatencyMeasurement {
    core: String,
    mean: String,
    ci: String,
    nb_iterations: usize,
    total_duration: String
}

impl CoreLatencyMeasurement {
    fn from(measurement: &Measurement) -> Self {
        Self {
            core: format!("Core#{}", measurement.memory_core),
            mean: format!("{:.3?}", measurement.mean_latency),
            ci: format!("{:.3?}", measurement.ci_mean_latency),
            nb_iterations: measurement.nb_iterations,
            total_duration: format!("{:.3?}", measurement.total_duration)
        }
    }
}
//endregion

/*
pub async fn hwloc_test() {
    fn print_children(topo: &Topology, obj: &TopologyObject, depth: usize) {
        let mut padding = std::iter::repeat("  ").take(depth)
            .collect::<String>();
        println!("{}{}: #{}", padding, obj, obj.os_index());

        for i in 0..obj.arity() {
            print_children(topo, obj.children()[i as usize], depth + 1);
        }
    }

    fn last_core(topo: &mut Topology) -> &TopologyObject {
        let core_depth = topo.depth_or_below_for_type(&ObjectType::Core).unwrap();
        let all_cores = topo.objects_at_depth(core_depth);
        all_cores.last().unwrap()
    }

    let mut topo = Topology::new().expect("This lib is really badly documented...");

    // ---------------------------------------------------------------------------------------------
    println!("*** Printing objects at different levels tree");
    for i in 0..topo.depth() {
        println!("*** Level {}", i);

        for (idx, object) in topo.objects_at_depth(i).iter().enumerate() {
            println!("{}: {}", idx, object);
        }
    }
    println!();

    // ---------------------------------------------------------------------------------------------
    println!("*** Printing overall tree");
    print_children(&topo, topo.object_at_root(), 0);

    // ---------------------------------------------------------------------------------------------
    println!("*** Pinning current thread of current process to a core");
    // Grab last core and exctract its CpuSet
    let mut cpuset = last_core(&mut topo).cpuset().unwrap();

    // Get only one logical processor (in case the core is SMT/hyper-threaded).
    cpuset.singlify();

    println!("Before Bind: {:?}", topo.get_cpubind(CpuBindFlags::CPUBIND_THREAD));
    // Last CPU Location for this PID (not implemented on all systems)
    if let Some(l) = topo.get_cpu_location(CpuBindFlags::CPUBIND_THREAD) {
        println!("Last Known CPU Location: {:?}", l);
    }
    println!();

    let sleep_duration = 5;
    println!("Sleeping {}s to see if task changes core", sleep_duration);
    let _ = tokio::time::sleep(Duration::from_secs(sleep_duration)).await;
    println!();
    // let _ = std::thread::sleep(Duration::from_secs(sleep_duration));
    println!("Before Bind: {:?}", topo.get_cpubind(CpuBindFlags::CPUBIND_THREAD));
    // Last CPU Location for this PID (not implemented on all systems)
    if let Some(l) = topo.get_cpu_location(CpuBindFlags::CPUBIND_THREAD) {
        println!("Last Known CPU Location: {:?}", l);
    }

    // Bind to one core.
    topo.set_cpubind(cpuset, CpuBindFlags::CPUBIND_THREAD);

    println!("After Bind: {:?}", topo.get_cpubind(CpuBindFlags::CPUBIND_THREAD));

    // Last CPU Location for this PID (not implemented on all systems)
    if let Some(l) = topo.get_cpu_location(CpuBindFlags::CPUBIND_THREAD) {
        println!("Last Known CPU Location: {:?}", l);
    }
}
*/

pub fn adapt_unit(size_b: usize) -> String {
    match size_b {
        s if s < KB => format!("{}B", s),
        s if s < MB => format!("{}KB", s >> KILO_OFFSET),
        s if s < GB => format!("{}MB", s >> MEGA_OFFSET),
        s => format!("{}GB", s >> GIGA_OFFSET),
    }
}

pub async fn random_data_init(size: usize, memory_core: usize) -> anyhow::Result<Vec<u8>> {
    let random_array = tokio::spawn(async move {

        // print!("Pinning memory to core {}...", memory_core);
        let pinned = core_affinity::set_for_current(CoreId{ id: memory_core });
        if !pinned {
            return Err(anyhow!("Unable to pin to memory core to CPU #{}", memory_core));
        }

        let mut rng = StdRng::seed_from_u64(SEED);
        let mut arr = vec![0; size];
        for i in 0..size {
            arr[i] = (rng.next_u32() & 0b11111111) as u8;
        }

        Ok(arr)
    }).await?;

    random_array
}

fn random_step_size_k(size: usize, arr: &mut Vec<u8>, nb_operations: usize) -> usize {
    let mut idx: usize = 0;

    for _ in 0..nb_operations {
        // Accesses a random cache line
        let pos = idx & (size - 1);
        arr[pos] += 1;
        idx += (arr[pos] * CACHE_LINE_SIZE) as usize;
    }

    return idx;
}

pub async fn numa_latency(
    execution_core: usize,
    memory_core: usize,
    array_size_bytes: usize,
    nb_operations: usize,
    nb_iterations: usize
) -> anyhow::Result<Measurement> {
    let target_duration = Duration::from_millis(500);
    let start = Instant::now();

    let pinned = core_affinity::set_for_current(CoreId{ id: execution_core });
    if !pinned {
        return Err(anyhow!("Unable to pin to execution core to CPU #{}", execution_core));
    }

    let mut random_array = random_data_init(array_size_bytes, memory_core).await?;

    let mut latencies = Vec::with_capacity(nb_iterations);
    // let mut results = Vec::with_capacity(nb_iterations);

    for i in 0..nb_iterations {
        let start_iteration = Instant::now();
        let _res = random_step_size_k(array_size_bytes, &mut random_array, nb_operations);
        latencies.push(start_iteration.elapsed());
        // results.push(_res);

        if start.elapsed() > target_duration && i > 100 {
            break;
        }
    }

    let total_duration = start.elapsed();

    Ok(Measurement::new(
        execution_core,
        memory_core,
        array_size_bytes,
        &latencies,
        total_duration,
        nb_operations))
}

pub async fn all_numa_latencies(nb_cores: usize, from_power: usize, to_power: usize) -> anyhow::Result<()> {

    let nb_sizes = to_power - from_power;
    let sizes_bytes = (from_power..to_power)
        .map(|power_of_two| ((1 << power_of_two) << KILO_OFFSET) as usize)
        .collect_vec();

    let mut measurements = Vec::with_capacity(nb_cores * nb_sizes);

    let mut core_measurements = vec![vec!(); nb_cores];
    let mut sizes_measurements = vec![vec!(); nb_sizes];

    for core in 0..nb_cores {
        let from_size = adapt_unit((1 << from_power) << KILO_OFFSET);
        let to_size = adapt_unit((1 << to_power) << KILO_OFFSET);
        eprint!("Benchmarking latency to core {} from {} to {}", core, from_size, to_size);

        for (size_index, array_size_bytes) in sizes_bytes.iter().enumerate() {
            // eprintln!("\n\tarray size = {}", adapt_unit(array_size_bytes));
            let measurement = numa_latency(0, core,
                *array_size_bytes, MILLION, 300).await?;

            core_measurements[core].push(SizeLatencyMeasurement::from(&measurement));
            sizes_measurements[size_index].push(CoreLatencyMeasurement::from(&measurement));
            measurements.push(measurement);
            eprint!(".")
        }
        println!();
    }

    println!("Latency per size: ====================================================================");
    for (core, measurements) in core_measurements.iter().enumerate() {
        println!("Core#{}:", core);
        println!("{}", Table::new(measurements.clone()).to_string());
        println!()
    }

    // println!("Latency per core : ===================================================================");
    // for (measurements, power_of_two) in sizes_measurements.iter().zip(from_power..to_power) {
    //     let array_size_bytes = (1 << power_of_two) << KILO_OFFSET;
    //     println!("Array size: {}", adapt_unit(array_size_bytes));
    //     println!("{}", Table::new(measurements.clone()).to_string());
    //     println!()
    // }

    // println!("All measurement: =====================================================================");
    // for measurement in measurements {
    //     println!("{}", measurement.to_json());
    // }
    /*
    let sizes_str = ['4KB', '8KB', '16KB', ...];
    let sizes = [4000, 8000, 16000, ...];
    let all_core_values = [
        ['Core#0', 2.282, 2.215, 2.198],
        ['Core#1', 2.218, 2.205, 2.200],
        ['Core#2', 2.195, 2.201, 2.194],
        ['Core#3', 2.229, 2.179, 2.200],
        ...
      ];
      // getconf -a | grep CACHE
     */
    let sizes_str = sizes_bytes.iter().map(|size| adapt_unit(*size)).collect_vec();
    println!("let sizes_str = {:?};", sizes_str);
    println!("let sizes = {:?};", sizes_bytes);
    println!("let all_core_values = [");
    for (core, measurements) in core_measurements.into_iter().enumerate() {
        print!("\t['Core#{}'", core);
        for measurement in measurements.into_iter() {
            let mut str = measurement.mean;
            str.retain(|c| !("nµms".contains(c)));
            print!(", {}", str);
        }
        println!("],");
    }
    println!("];");


    Ok(())
}
//endregion

//region Transaction sizes -------------------------------------------------------------------------
pub fn transaction_sizes(nb_schedulers: usize, nb_workers: usize) {
    println!("Reference:");
    // println!("\tu32 ~ {}B", mem::size_of::<u32>());
    // println!("\tu64 ~ {}B", mem::size_of::<u64>());
    // println!("\tusize ~ {}B", mem::size_of::<usize>());
    // println!("\tAtomicFunction ~ {}B", mem::size_of::<AtomicFunction>());
    // println!("\tOption<u32> ~ {}B", mem::size_of::<Option<u32>>());
    // println!("\tOption<u64> ~ {}B", mem::size_of::<Option<u64>>());
    // println!("\tOption<usize> ~ {}B", mem::size_of::<Option<usize>>());
    // println!();

    // let size = mem::size_of::<[AccessPattern; 2]>();
    // println!("Size of [_; 2]: {}", size);
    //
    // let size_option = mem::size_of::<Option<[AccessPattern; 2]>>();
    // println!("Size of Option<[_; 2]>: {}", size_option);

    fn sizes<const NB_ADDRESSES: usize, const NB_PARAMS: usize>(nb_schedulers: usize, nb_workers: usize) {
        let tx_size_bytes = mem::size_of::<Transaction<NB_ADDRESSES, NB_PARAMS>>();
        let batch_size = 65536;
        let batch_size_bytes = batch_size * tx_size_bytes;
        let chunk_size_bytes = batch_size_bytes / nb_schedulers;
        let executor_share_bytes = chunk_size_bytes / nb_workers;
        println!("Size tx<{}, {}> ~ {} B \n\tbatch: {}, chunk: {}, worker_share: {}",
                 NB_ADDRESSES, NB_PARAMS, tx_size_bytes, adapt_unit(batch_size_bytes),
                 adapt_unit(chunk_size_bytes), adapt_unit(executor_share_bytes));
    }


    println!("{} schedulers, {} workers", nb_schedulers, nb_workers);
    sizes::<0, 1>(nb_schedulers, nb_workers);
    sizes::<2, 1>(nb_schedulers, nb_workers);
    sizes::<2, 10>(nb_schedulers, nb_workers);
    sizes::<2, 20>(nb_schedulers, nb_workers);
    sizes::<2, 26>(nb_schedulers, nb_workers);
    println!();

    // let tx_size_bytes = mem::size_of::<Arc<Transaction<2, 10>>>();
    // println!("Arc size = {} B", tx_size_bytes);


    // Need at least 8 schedulers so that a chunk can fit in L2 memory -> at most 24 executors
    // Target chunk sizes that can fill L2 cache?
    // Worker share seems to always be able to fit in L1
    // => most of L2 cache of worker will serve to keep storage hot
}
//endregion

//region scheduling --------------------------------------------------------------------------------
pub fn profile_schedule_chunk(batch_size: usize, iter: usize, chunk_fraction: usize, nb_executors: usize, conflict_rate: f64) {

    let storage_size = 100 * batch_size;

    let addresses_per_tx = 2;
    // let addresses_per_tx = 10;

    let mut rng = StdRng::seed_from_u64(10);
    let mut batch = WorkloadUtils::transfer_pairs(storage_size, batch_size, conflict_rate, &mut rng)
        .iter()
        .enumerate()
        .map(|(tx_index, pair)| {
            Transaction {
                sender: pair.0 as SenderAddress,
                function: AtomicFunction::Transfer,
                tx_index,
                addresses: [pair.0, pair.1],
                // addresses: [tx_index as StaticAddress, 1 + tx_index as StaticAddress], // Transfer loop
                params: [2],
            }
            // Transaction {
            //     sender: pair.0 as SenderAddress,
            //     function: AtomicFunction::Fibonacci,
            //     tx_index,
            //     addresses: [],
            //     params: [10],
            // }
        }).collect_vec();

    let mut sequential = SequentialVM::new(100 * batch.len()).unwrap();
    sequential.storage.fill(20 * iter as Word);

    let mut parallel = ParallelVmImmediate::new(storage_size, 1, nb_executors).unwrap();
    parallel.set_storage(20 * iter as Word);

    let mut schedule_duration = Duration::from_nanos(0);
    let mut schedule_init_duration = Duration::from_nanos(0);
    let mut schedule_main_loop_duration = Duration::from_nanos(0);

    let mut sequential_duration = Duration::from_nanos(0);
    let mut parallel_duration = Duration::from_nanos(0);

    let mut experiment_duration = Duration::from_nanos(0);
    let mut experiment_init_duration = Duration::from_nanos(0);
    let mut experiment_scheduling_duration = Duration::from_nanos(0);

    let mut assign_duration = Duration::from_nanos(0);
    let mut assign_init_duration = Duration::from_nanos(0);
    let mut assign_scheduling_duration = Duration::from_nanos(0);

    let computation = |(scheduler_index, chunk): (usize, Vec<Transaction<2, 1>>)| {
        let a = Instant::now();
        let mut scheduled = Vec::with_capacity(chunk.len());
        let mut postponed = Vec::with_capacity(chunk.len());
        let mut working_set = AddressSet::with_capacity(
            2 * addresses_per_tx * batch_size / chunk_fraction
        );

        let init = a.elapsed();
        let main_loop = Instant::now();
        'outer: for tx in chunk {
            if tx.function != AtomicFunction::KeyValue(KeyValueOperation::Scan) {
                for addr in tx.addresses.iter() {
                    if !working_set.insert(*addr) {
                        // Can't add tx to schedule
                        postponed.push(tx);
                        continue 'outer;
                    }
                }
            } else if !tx.addresses.is_empty() {
                eprintln!("Processing a scan operation");
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
        (scheduled, postponed, init, main_loop.elapsed())
    };

    let experiment = |(scheduler_index, chunk): (usize, Vec<Transaction<2, 1>>)| {
        let init_start = Instant::now();
        let mut scheduled = Vec::with_capacity(chunk.len());
        let mut postponed = Vec::with_capacity(chunk.len());

        let mut some_reads = false;
        let mut some_writes = false;

        let mut read_locked = false;
        let mut write_locked = false;

        let mut address_map_capacity = addresses_per_tx * batch_size / chunk_fraction;
        address_map_capacity *= 2;

        // type Map = HashMap<StaticAddress, AccessType>;
        // let mut address_map: HashMap<StaticAddress, AccessType> = HashMap::with_capacity(address_map_capacity);

        // type Map = HashMap<StaticAddress, AccessType, BuildHasherDefault<NoHashHasher<StaticAddress>>>;
        // let mut address_map: Map = HashMap::with_capacity_and_hasher(
        //     address_map_capacity, BuildNoHashHasher::default());

        // type Map = ThinMap<StaticAddress, AccessType>;
        // let mut address_map: ThinMap<StaticAddress, AccessType> = ThinMap::with_capacity(address_map_capacity);

        type Map = ThinMap<StaticAddress, AccessType, BuildHasherDefault<NoHashHasher<StaticAddress>>>;
        let mut address_map: Map = ThinMap::with_capacity_and_hasher(address_map_capacity, BuildNoHashHasher::default());

        let mut can_read = |addr: StaticAddress, address_map: &mut Map| {
            let entry = address_map.entry(addr).or_insert(AccessType::Read);
            *entry == AccessType::Read
        };
        let mut can_write = |addr: StaticAddress, address_map: &mut Map| {
            let entry = address_map.entry(addr).or_insert(AccessType::TryWrite);
            if *entry == AccessType::TryWrite {
                *entry = AccessType::Write;
                true
            } else {
                false
            }
        };

        let init_duration = init_start.elapsed();
        let scheduling = Instant::now();
        'backlog: for tx in chunk {
            // // Positive = read
            // // i32::MAX -> read all
            // // Negative = write
            // // i32::MIN -> write all (i.e. exclusive)

            // let tmp_writes = Some([
            //     AccessPattern::Address(tx.addresses[0]),
            //     AccessPattern::Address(tx.addresses[1]),
            // ]);
            // // let tmp_writes = Some([
            // //     AccessPattern::Address(tx.addresses[0]),
            // //     AccessPattern::Address(tx.addresses[1]),
            // //     AccessPattern::Range(tx.addresses[0], tx.addresses[0] + 10),
            // //     AccessPattern::Done,
            // // ]);
            // let tmp_reads: Option<[AccessPattern; 2]> = None;
            //
            // let reads = tmp_reads.as_ref().map_or([].as_slice(), |inside| inside.as_slice());
            // let writes = tmp_writes.as_ref().map_or([].as_slice(), |inside| inside.as_slice());

            let (possible_reads, possible_writes) = tx.accesses();
            let reads = possible_reads.as_ref().map_or([].as_slice(), |inside| inside.as_slice());
            let writes = possible_writes.as_ref().map_or([].as_slice(), |inside| inside.as_slice());

            // Tx without any memory accesses
            if reads.is_empty() && writes.is_empty() {
                scheduled.push(tx);
                continue 'backlog;
            }

            if read_locked {
                // Only reads are allowed
                if writes.is_empty() { scheduled.push(tx); }
                else { postponed.push(tx); }
                continue 'backlog;
            }

            if write_locked {
                postponed.push(tx);
                continue 'backlog;
            }

            // NB: A tx might be postponed after having some of its addresses added to the address set
            // It is probably too expensive to rollback those changes. TODO Check
            // Start with reads because they are less problematic if the tx is postponed
            'reads: for read in reads {
                // println!("\tchecking reads");
                match read {
                    AccessPattern::Address(addr) => {
                        if !can_read(*addr, &mut address_map) {
                            postponed.push(tx);
                            continue 'backlog;
                        }
                    },
                    AccessPattern::Range(from, to) => {
                        assert!(from < to);
                        'range: for addr in (*from)..(*to) {
                            if !can_read(addr, &mut address_map) {
                                postponed.push(tx);
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
                        continue 'backlog;
                    },
                    AccessPattern::Done => {
                        break 'reads;
                    }
                }
            }

            'writes: for write in writes {
                // println!("\tchecking writes");
                match write {
                    AccessPattern::Address(addr) => {
                        // println!("\t\twrite single address");
                        if !can_write(*addr, &mut address_map) {
                            postponed.push(tx);
                            continue 'backlog;
                        }
                    },
                    AccessPattern::Range(from, to) => {
                        assert!(from < to);
                        // println!("\t\twrite from {} to {}", from, to);
                        'range: for addr in (*from)..(*to) {
                            if !can_write(addr, &mut address_map) {
                                postponed.push(tx);
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
                        break 'writes;
                    }
                }
            }

            // Transaction can be executed in parallel
            scheduled.push(tx);
        }

        // println!("Scheduled: {}, postponed: {}", scheduled.len(), postponed.len());
        (scheduled, postponed, init_duration, scheduling.elapsed())
    };

    let assign = |transactions: Vec<Transaction<2, 1>>| {
        let a = Instant::now();
        let chunk_size = batch_size / chunk_fraction;
        let mut address_map: HashMap<StaticAddress, Assignment, BuildHasherDefault<AHasher>> =
            HashMap::with_capacity_and_hasher(2 * addresses_per_tx * chunk_size, BuildHasherDefault::default());
        let mut read_only: Vec<usize> = vec!();
        let mut exclusive: Vec<usize> = vec!();
        let mut scheduled: Vec<Vec<usize>>= vec![Vec::with_capacity(chunk_size/nb_executors); nb_executors];
        let mut postponed: Vec<usize> = vec!();
        let mut new_reads: Vec<StaticAddress> = Vec::with_capacity(addresses_per_tx);
        let mut new_writes: Vec<StaticAddress> = Vec::with_capacity(addresses_per_tx);
        // println!("{}", read_only.capacity());
        // println!("{}", exclusive.capacity());
        // println!("{}", scheduled.capacity());
        // println!("{}", postponed.capacity());
        // println!("{}", new_reads.capacity());
        // println!("{}", new_writes.capacity());
        // TODO Check schedule
        let init_duration = a.elapsed();

        let main_scheduling = Instant::now();
        Scheduling::schedule_chunk_assign(
            &transactions,
            &mut address_map,
            &mut read_only,
            &mut exclusive,
            &mut scheduled,
            &mut postponed,
            &mut new_reads,
            &mut new_writes);
        let postponed_tx = postponed.iter().map(|index| *transactions.get(*index).unwrap()).collect_vec();
        (scheduled, postponed_tx, init_duration, main_scheduling.elapsed())
    };

    batch.truncate(batch_size/chunk_fraction);

    for _ in 0..iter {
        let mut b = batch.clone();
        let a = Instant::now();
        let (_, postponed, init, main_loop) = computation((0, b));
        // println!("{:?} txs were postponed", postponed.len());
        schedule_duration += a.elapsed();
        schedule_init_duration += init;
        schedule_main_loop_duration += main_loop;

        // let mut b = batch.clone();
        // let a = Instant::now();
        // let _ = sequential.execute(b);
        // sequential_duration += a.elapsed();
        //
        // let mut b = batch.clone();
        // let a = Instant::now();
        // let _ = parallel.vm.execute_round(b);
        // parallel_duration += a.elapsed();

        let mut b = batch.clone();
        let a = Instant::now();
        let (_, postponed, init, experiment_scheduling) = experiment((0, b));
        // println!("{:?} txs were postponed", postponed.len());
        experiment_duration += a.elapsed();
        experiment_init_duration += init;
        experiment_scheduling_duration += experiment_scheduling;

        let mut b = batch.clone();
        let a = Instant::now();
        let (scheduled, postponed, init, assign_scheduling) = assign(b);
        // for (worker_index, worker) in scheduled.iter().enumerate() {
        //     println!("worker {} was assigned {:?} tx", worker_index, worker);
        // }
        // println!("{:?} txs were postponed", postponed.len());
        assign_duration += a.elapsed();
        assign_init_duration += init;
        assign_scheduling_duration += assign_scheduling;
    }

    println!("For a chunk of {} tx among {} executors", batch.len(), nb_executors);
    println!("\tschedule_chunk latency = {:?} (init = {:?}, rest = {:?})", schedule_duration / (iter as u32),
             schedule_init_duration / (iter as u32),
             schedule_main_loop_duration / (iter as u32));
    println!("\t**experimental scheduling = {:?} (init = {:?}, rest = {:?})",
             experiment_duration / (iter as u32),
             experiment_init_duration / (iter as u32),
             experiment_scheduling_duration / (iter as u32));

    println!("\t**scheduling/assign = {:?} (init = {:?}, rest = {:?})",
             assign_duration / (iter as u32),
             assign_init_duration / (iter as u32),
             assign_scheduling_duration / (iter as u32));

    // let avg_sequential = sequential_duration / (iter as u32);
    // println!("\tsequential exec latency = {:?} -> full batch should take {:?}", avg_sequential, avg_sequential * (chunk_fraction as u32));
    //
    // let avg_parallel = parallel_duration / (iter as u32);
    // println!("\tparallel exec latency = {:?} -> full batch should take {:?}", avg_parallel, avg_parallel * (chunk_fraction as u32));
    println!();
}
//endregion

//region Benchmarking HashMaps ---------------------------------------------------------------------
pub fn bench_hashmaps(nb_iter: usize, addr_per_tx: usize, batch_size: usize) {
    println!("Batch of {} tx with {} addr per tx = {} insertions ({} reps)", batch_size, addr_per_tx, batch_size * addr_per_tx, nb_iter);
    let mut rng = StdRng::seed_from_u64(10);
    let storage_size = 100 * batch_size;
    let mut addresses: Vec<StaticAddress> = (0..storage_size).map(|el| el as StaticAddress).collect();
    addresses.shuffle(&mut rng);
    addresses.truncate(addr_per_tx * batch_size);
    let map_capacity = 2 * addresses.len();
    
    let mut measurements = vec!();

    measurements.push(HashMap::<StaticAddress, StaticAddress>::measure(nb_iter, &addresses));
    measurements.push(HashMapNoHash::measure(nb_iter, &addresses));
    measurements.push(HashMapAHash::measure(nb_iter, &addresses));

    measurements.push(BrownMap::<StaticAddress, StaticAddress>::measure(nb_iter, &addresses));
    measurements.push(BrownMapNoHash::measure(nb_iter, &addresses));

    measurements.push(ThinMap::<StaticAddress, StaticAddress>::measure(nb_iter, &addresses));
    measurements.push(ThinMapNoHash::measure(nb_iter, &addresses));
    measurements.push(ThinMapAHash::measure(nb_iter, &addresses));

    println!("{}", Table::new(measurements).to_string());
    println!();

    //Expected results: https://github.com/rust-lang/hashbrown
}

pub fn bench_hashmaps_clear(nb_iter: usize, addr_per_tx: usize, batch_size: usize) {
    println!("Batch of {} tx with {} addr per tx = {} insertions ({} reps)", batch_size, addr_per_tx, batch_size * addr_per_tx, nb_iter);
    let mut rng = StdRng::seed_from_u64(10);
    let storage_size = 100 * batch_size;
    let mut addresses: Vec<StaticAddress> = (0..storage_size).map(|el| el as StaticAddress).collect();
    addresses.shuffle(&mut rng);
    addresses.truncate(addr_per_tx * batch_size);
    let map_capacity = 2 * addresses.len();

    let mut measurements = vec!();

    measurements.push(HashMap::<StaticAddress, StaticAddress>::measure_clear(nb_iter, &addresses));
    measurements.push(HashMapNoHash::measure_clear(nb_iter, &addresses));
    measurements.push(HashMapAHash::measure_clear(nb_iter, &addresses));

    measurements.push(BrownMap::<StaticAddress, StaticAddress>::measure_clear(nb_iter, &addresses));
    measurements.push(BrownMapNoHash::measure_clear(nb_iter, &addresses));

    measurements.push(ThinMap::<StaticAddress, StaticAddress>::measure_clear(nb_iter, &addresses));
    measurements.push(ThinMapNoHash::measure_clear(nb_iter, &addresses));
    measurements.push(ThinMapAHash::measure_clear(nb_iter, &addresses));

    println!("{}", Table::new(measurements).to_string());
    println!();

    //Expected results: https://github.com/rust-lang/hashbrown
}

trait HashMapWrapper {
    fn print_name() -> String where Self: Sized;
    fn with_capacity(capacity: usize) -> Self where Self: Sized;
    fn insert(&mut self, key: StaticAddress, value: StaticAddress) -> Option<StaticAddress>;
    fn clear(&mut self);
    fn measure(nb_iter: usize, to_insert: &Vec<StaticAddress>) -> HashMapMeasurement where Self: Sized {
        
        let mut durations = Vec::with_capacity(nb_iter);
        let capacity = 2 * to_insert.len(); // capacity must be slightly larger than the number of value to insert
        
        for _ in 0..nb_iter {
            let mut map: Box<dyn HashMapWrapper> = Box::new(Self::with_capacity(capacity));

            let start = Instant::now();
            for addr in to_insert.iter() {
                map.insert(*addr, *addr);
            }
            durations.push(start.elapsed());
        }
        let (mean, ci) = mean_ci(&durations);

        HashMapMeasurement {
            hash_map_type: Self::print_name(),
            mean_latency: format!("{:?}", mean),
            ci: format!("{:?}", ci),
        }
    }

    fn measure_clear(nb_iter: usize, to_insert: &Vec<StaticAddress>) -> HashMapMeasurement where Self: Sized {

        let mut durations = Vec::with_capacity(nb_iter);
        let capacity = 2 * to_insert.len(); // capacity must be slightly larger than the number of value to insert

        for _ in 0..nb_iter {
            let mut map: Box<dyn HashMapWrapper> = Box::new(Self::with_capacity(capacity));

            for addr in to_insert.iter() {
                map.insert(*addr, *addr);
            }
            let start = Instant::now();
            map.clear();
            durations.push(start.elapsed());
        }
        let (mean, ci) = mean_ci(&durations);

        HashMapMeasurement {
            hash_map_type: Self::print_name(),
            mean_latency: format!("{:?}", mean),
            ci: format!("{:?}", ci),
        }
    }
}

#[derive(Tabled)]
struct HashMapMeasurement {
    pub hash_map_type: String,
    pub mean_latency: String,
    pub ci: String,
}

//region Hash map wrappers
// HashMap
impl HashMapWrapper for HashMap<StaticAddress, StaticAddress> {
    fn print_name() -> String where Self: Sized {
        format!("HashMap")
    }

    fn with_capacity(capacity: usize) -> Self where Self: Sized {
        HashMap::with_capacity(capacity)
    }

    fn insert(&mut self, key: StaticAddress, value: StaticAddress) -> Option<StaticAddress> {
        self.insert(key, value)
    }

    fn clear(&mut self) {
        self.clear();
    }
}

type HashMapNoHash = HashMap<StaticAddress, StaticAddress, BuildHasherDefault<NoHashHasher<StaticAddress>>>;
impl HashMapWrapper for HashMapNoHash {
    fn print_name() -> String where Self: Sized {
        format!("HashMap (NoHash)")
    }
    fn with_capacity(capacity: usize) -> Self where Self: Sized {
        HashMap::with_capacity_and_hasher(capacity, BuildNoHashHasher::default())
    }

    fn insert(&mut self, key: StaticAddress, value: StaticAddress) -> Option<StaticAddress> {
        self.insert(key, value)
    }

    fn clear(&mut self) {
        self.clear();
    }
}

type HashMapAHash = HashMap<StaticAddress, StaticAddress, BuildHasherDefault<AHasher>>;
impl HashMapWrapper for HashMapAHash {
    fn print_name() -> String where Self: Sized {
        format!("HashMap (AHash)")
    }
    fn with_capacity(capacity: usize) -> Self where Self: Sized {
        HashMap::with_capacity_and_hasher(capacity, BuildHasherDefault::default())
    }

    fn insert(&mut self, key: StaticAddress, value: StaticAddress) -> Option<StaticAddress> {
        self.insert(key, value)
    }

    fn clear(&mut self) {
        self.clear();
    }
}

// BrownMap
impl HashMapWrapper for BrownMap<StaticAddress, StaticAddress> {
    fn print_name() -> String where Self: Sized {
        format!("BrownMap")
    }
    
    fn with_capacity(capacity: usize) -> Self where Self: Sized {
        BrownMap::with_capacity(capacity)
    }

    fn insert(&mut self, key: StaticAddress, value: StaticAddress) -> Option<StaticAddress> {
        self.insert(key, value)
    }

    fn clear(&mut self) {
        self.clear();
    }
}

type BrownMapNoHash = BrownMap<StaticAddress, StaticAddress, BuildHasherDefault<NoHashHasher<StaticAddress>>>;
impl HashMapWrapper for BrownMapNoHash {
    fn print_name() -> String where Self: Sized {
        format!("BrownMap (NoHash)")
    }
    
    fn with_capacity(capacity: usize) -> Self where Self: Sized {
        BrownMap::with_capacity_and_hasher(capacity, BuildNoHashHasher::default())
    }

    fn insert(&mut self, key: StaticAddress, value: StaticAddress) -> Option<StaticAddress> {
        self.insert(key, value)
    }

    fn clear(&mut self) {
        self.clear();
    }
}

// ThinMap
impl HashMapWrapper for ThinMap<StaticAddress, StaticAddress> {
    fn print_name() -> String where Self: Sized {
        format!("ThinMap")
    }
    
    fn with_capacity(capacity: usize) -> Self where Self: Sized {
        ThinMap::with_capacity(capacity)
    }

    fn insert(&mut self, key: StaticAddress, value: StaticAddress) -> Option<StaticAddress> {
        self.insert(key, value)
    }

    fn clear(&mut self) {
        self.clear();
    }
}

type ThinMapNoHash = ThinMap<StaticAddress, StaticAddress, BuildHasherDefault<NoHashHasher<StaticAddress>>>;
impl HashMapWrapper for ThinMapNoHash {
    fn print_name() -> String where Self: Sized {
        format!("ThinMap (NoHash)")
    }
    
    fn with_capacity(capacity: usize) -> Self where Self: Sized {
        ThinMap::with_capacity_and_hasher(capacity, BuildNoHashHasher::default())
    }

    fn insert(&mut self, key: StaticAddress, value: StaticAddress) -> Option<StaticAddress> {
        self.insert(key, value)
    }

    fn clear(&mut self) {
        self.clear();
    }
}

type ThinMapAHash = ThinMap<StaticAddress, StaticAddress, BuildHasherDefault<AHasher>>;
impl HashMapWrapper for ThinMapAHash {
    fn print_name() -> String where Self: Sized {
        format!("ThinMap (AHash)")
    }
    
    fn with_capacity(capacity: usize) -> Self where Self: Sized {
        ThinMap::with_capacity_and_hasher(capacity, BuildHasherDefault::default())
    }

    fn insert(&mut self, key: StaticAddress, value: StaticAddress) -> Option<StaticAddress> {
        self.insert(key, value)
    }

    fn clear(&mut self) {
        self.clear();
    }
}
//endregion

//endregion