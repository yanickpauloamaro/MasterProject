use hwloc::{Topology, CPUBIND_PROCESS, TopologyObject, ObjectType};
use crate::transaction::TransactionType;
use std::fmt::Display;

#[derive(Debug)]
pub struct Config {
    pub rate: u32, // in ops/s
    pub batch_size: u32, // in ops
    pub nb_nodes: Option<usize>, // None => all available
    pub address_space_size: usize, // in byte, will be split between NUMA regions
    pub transaction_type: TransactionType,
}

fn compatible(topo: &Topology) -> bool {
    let checks = [
        // Check if Process Binding for CPUs is supported
        ("CPU Binding (current process)", topo.support().cpu().set_current_process()),
        ("CPU Binding (any process)", topo.support().cpu().set_process()),
        // Check if Thread Binding for CPUs is supported
        ("CPU Binding (current thread)", topo.support().cpu().set_current_thread()),
        ("CPU Binding (any thread)", topo.support().cpu().set_thread()),
    ];

    let mut res = true;

    for check in checks {
        if !check.1 {
            println!("{} not supported", check.0);
            res = false;
        }
    }

    return res;
}

pub fn get_nb_cores(topo: &Topology) -> usize {
    let core_depth = topo.depth_or_below_for_type(&ObjectType::Core).unwrap();
    let all_cores = topo.objects_at_depth(core_depth);
    return all_cores.len();
}

pub fn benchmark(config: Config) {
    let topo = Topology::new();

    if !compatible(&topo) {
        return;
    }

    let nb_cores = get_nb_cores(&topo);
    let nb_nodes  = match config.nb_nodes {
        Some(nb_nodes) if nb_nodes > nb_cores => {
            eprintln!("Not enough cores to run benchmark. {} cores requested but only {} cores available", nb_nodes, nb_cores);
            return;
        },
        Some(nb_nodes) => nb_nodes,
        None => nb_cores
    };

    let share = config.address_space_size / nb_nodes;

    println!("Benchmarking!");
    // Allocate "VM address space" (split per NUMA region?)
    // Create nodes (represent NUMA regions?)
        // Set memory policy so that memory is allocated locally to each core
        // TODO Check how to send transaction to each node
    // Create Backlog
    // Create dispatcher (will send the transactions to the different regions)
    // Create client (will generate transactions)
}