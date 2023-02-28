// use crate::transaction::TransactionType;
use crate::config::Config;

use hwloc::{Topology, ObjectType};
// use hwloc::{Topology, CPUBIND_PROCESS, TopologyObject, ObjectType};
use anyhow::{self, Result};


fn check(condition: bool, ctx: &str) -> Result<()> {

    if condition {
        Ok(())
    } else {
        let error_msg = anyhow::anyhow!("Host not compatible. {} not supported", ctx);
        Err(error_msg)
    }
}

fn compatible(topo: &Topology) -> Result<()> {

    check(topo.support().cpu().set_current_process(), "CPU Binding (current process)")?;
    check(topo.support().cpu().set_process(), "CPU Binding (any process)")?;
    check(topo.support().cpu().set_current_thread(), "CPU Binding (current thread)")?;
    check(topo.support().cpu().set_thread(), "CPU Binding (any thread)")?;

    Ok(())
}

pub fn get_nb_cores(topo: &Topology) -> usize {
    let core_depth = topo.depth_or_below_for_type(&ObjectType::Core).unwrap();
    let all_cores = topo.objects_at_depth(core_depth);
    return all_cores.len();
}

pub fn benchmark(config: Config) -> Result<()> {
    let topo = Topology::new();

    // Check compatibility with core pinning
    compatible(&topo)?;

    // Determine number of cores to use
    let nb_cores = get_nb_cores(&topo);
    let nb_nodes  = match config.nb_nodes {
        Some(nb_nodes) if nb_nodes > nb_cores => {
            let error_msg = anyhow::anyhow!(
                "Not enough cores to run benchmark. {} requested but only {} available",
                nb_nodes, nb_cores);
            return Err(error_msg);
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


    config.save("config.json")
    // let mut file = File::open("text.json").unwrap();
    // let object = config.clone();
    // let str = serde_json::to_string(&object).unwrap();
    // println!("{}", str);
    //
    // let reconstructed: Config = serde_json::from_str(&str).unwrap();
    // println!("{:?}", reconstructed);

    // let the_file = /* ... */;
    // let person: Person = serde_json::from_str(the_file).expect("JSON was not well-formatted");
}