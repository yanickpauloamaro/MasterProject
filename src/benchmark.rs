use std::io::Write;
use hwloc::Topology;
// use hwloc::{Topology, CPUBIND_PROCESS, TopologyObject, ObjectType};
use anyhow::{self, Context, Result};
use std::time::Duration;
use tokio::time::Instant;
use std::mem;
use async_trait::async_trait;
use bloomfilter::Bloom;
use num_traits::FromPrimitive;
use crate::baseline_vm::SerialVM;
use crate::basic_vm::BasicVM;
use crate::utils::{account_creation_batch, compatible, create_batch_partitioned, get_nb_nodes, print_metrics, print_throughput, transaction_loop};
use crate::vm::{Batch, ExecutionResult, Jobs, VM};
use crate::config::Config;
use crate::transaction::Transaction;
use crate::wip::BloomVM;

pub enum VmWrapper {
    Basic(BasicVM),
    Serial(SerialVM),
    Bloom(BloomVM)
}

impl VmWrapper {
    async fn execute(&mut self, mut backlog: Jobs) -> Result<Vec<ExecutionResult>> {
        match self {
            Self::Basic(vm) => vm.execute(backlog).await,
            Self::Bloom(vm) => vm.execute(backlog).await,
            Self::Serial(vm) => vm.execute(backlog).await,
        }
    }
}

pub trait Workload {
    fn new(config: Config) -> Self where Self: Sized;
    fn new_vm(&self) -> Result<VmWrapper>;
    fn init(&self) -> Vec<Jobs>;
    fn load(&self) -> Vec<Jobs>;
    // TODO Check that the final state is valid -> need the state as parameter
    fn check(&self) -> bool;
}

pub struct TransactionLoop {
    nb_accounts: usize,
    config: Config,
}
impl Workload for TransactionLoop {
    fn new(config: Config) -> Self where Self: Sized {
        return Self{ nb_accounts: 4, config };
    }

    fn new_vm(&self) -> Result<VmWrapper> {
        // TODO Refactor this
        let topo = Topology::new();
        // Check compatibility with core pinning
        compatible(&topo)?;
        let nb_nodes = get_nb_nodes(&topo, &self.config)?;
        // let share = config.address_space_size / nb_nodes;

        // TODO Choose VM from config
        // let mut vm = BasicVM::new(nb_nodes, config.batch_size);
        // let mut vm = BloomVM::new(nb_nodes, config.batch_size);
        let vm = SerialVM::new(nb_nodes, self.config.batch_size);

        return Ok(VmWrapper::Serial(vm));
    }

    fn init(&self) -> Vec<Jobs> {
        let create_accounts = account_creation_batch(self.nb_accounts, self.nb_accounts, 100);
        return vec!(create_accounts);
    }

    fn load(&self) -> Vec<Jobs> {
        let jobs = transaction_loop(self.nb_accounts, self.nb_accounts);
        return vec!(jobs);
    }

    fn check(&self) -> bool {
        return true;
    }
}

pub struct BasicWorkload {
    nb_accounts: usize,
    config: Config,
}
impl Workload for BasicWorkload {
    fn new(config: Config) -> Self where Self: Sized {
        // TODO replace nb_accounts by the number of workers in the VM?
        return Self{ nb_accounts: 4, config };
    }

    fn new_vm(&self) -> Result<VmWrapper> {
        let topo = Topology::new();
        // Check compatibility with core pinning
        compatible(&topo)?;
        let nb_nodes = get_nb_nodes(&topo, &self.config)?;
        // let share = config.address_space_size / nb_nodes;

        // TODO Choose VM from config
        // let mut vm = BasicVM::new(nb_nodes, self.config.batch_size);
        let vm = VmWrapper::Bloom(BloomVM::new(nb_nodes, self.config.batch_size));
        // let vm = VmWrapper::Serial(SerialVM::new(nb_nodes, self.config.batch_size););

        return Ok(vm);
    }

    fn init(&self) -> Vec<Jobs> {
        let mut max = 0;
        let chunks = (self.config.batch_size / self.nb_accounts) as u64;
        for i in 0..self.config.batch_size {
            let from = i as u64 / chunks;
            let to = ((self.nb_accounts + i) % (2 * self.nb_accounts)) as u64;
            let amount = 20;
            if from > max { max = from; }
            if to > max { max = to; }
        }

        let create_accounts = account_creation_batch(
            self.config.batch_size,
            max as usize + 1,
            100 * self.config.batch_size as u64
        );
        println!("Max address: {}", max);
        return vec!(create_accounts);
    }

    fn load(&self) -> Vec<Jobs> {
        let jobs = create_batch_partitioned(self.config.batch_size, self.nb_accounts);
        return vec!(jobs);
    }

    fn check(&self) -> bool {
        return true;
    }
}

#[async_trait]
pub trait Benchmark {
    async fn run(config: Config, nb_iter: usize) -> Result<()>;
}

#[async_trait]
impl<L: 'static> Benchmark for L where L: Workload + Send  {
    // TODO Include VM choice as parameter in config
    async fn run(config: Config, nb_iter: usize) -> Result<()>
    {
        // Setup ---------------------------------------------------------------------------------------
        let workload = L::new(config);

        let mut vm = workload.new_vm()?;
        // vm.prepare().await;

        return tokio::spawn(async move {
            // Preparation -----------------------------------------------------------------------------
            print!("Preparing workload...");
            let prep_start = Instant::now();
            let prep = workload.init();
            for batch in prep {
                vm.execute(batch).await?;
            }
            println!("Done. Took {:?}", prep_start.elapsed());

            // Benchmark ---------------------------------------------------------------------------
            println!();
            println!("Benchmarking:");
            for iter in 0..nb_iter {
                println!("Iteration {}:", iter);
                let load = workload.load();
                let nb_batches = load.len();
                let mut nb_transactions = 0;

                let start = Instant::now();
                for batch in load {
                    nb_transactions += batch.len();

                    let result = vm.execute(batch).await?;
                }
                let duration = start.elapsed();

                if workload.check() {
                    print_throughput(nb_batches, nb_transactions, duration);
                } else {
                    println!("Incorrect benchmark result");
                }
                println!();
            }

            Ok(())
        }).await?;
    }
}