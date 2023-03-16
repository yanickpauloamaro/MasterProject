use std::io::Write;

use rand::thread_rng;
use rand::seq::SliceRandom;
use hwloc::Topology;
// use hwloc::{Topology, CPUBIND_PROCESS, TopologyObject, ObjectType};
use anyhow::{anyhow, Context, Result};
use std::time::Duration;
use tokio::time::Instant;
use std::mem;
use async_trait::async_trait;
use bloomfilter::Bloom;
use num_traits::FromPrimitive;
use tokio::runtime::Runtime;
use crate::baseline_vm::SerialVM;
use crate::basic_vm::BasicVM;
use crate::utils::{account_creation_batch, batch_with_conflicts, compatible, ContentionGenerator, create_batch_from_generator, create_batch_partitioned, get_nb_nodes, print_conflict_rate, print_metrics, print_throughput, transaction_loop, transfer};
use crate::vm::{Batch, ExecutionResult, Jobs, VM};
use crate::config::Config;
use crate::transaction::{Instruction, Transaction, TransactionAddress};
use crate::vm_implementation::{VMa, VMb, VMc};
use crate::wip::{BloomVM, Executor, Word};
use crate::worker_implementation::{WorkerBStd, WorkerBTokio};

pub enum VmWrapper {
    // Basic(BasicVM),
    // Serial(SerialVM),
    // Bloom(BloomVM),
    A(VMa),
    B(VMb<WorkerBTokio>),
    C(VMc)
}

impl VmWrapper {
    fn execute(&mut self, mut backlog: Jobs) -> Result<Vec<ExecutionResult>> {
        match self {
            // Self::Basic(vm) => vm.execute(backlog).await,
            // Self::Bloom(vm) => vm.execute(backlog).await,
            // Self::Serial(vm) => vm.execute(backlog).await,
            Self::A(vm) => vm.execute(backlog),
            Self::B(vm) => vm.execute(backlog),
            Self::C(vm) => vm.execute(backlog),
        }
    }

    fn init(&mut self, mut jobs: Jobs) -> Result<Vec<ExecutionResult>> {
        match self {
            // Self::Basic(vm) => Ok(vec!()),
            // Self::Bloom(vm) => vm.init(jobs).await,
            // Self::Serial(vm) => vm.execute(jobs).await,
            Self::A(vm) => vm.execute(jobs),
            Self::B(vm) => vm.execute(jobs),
            Self::C(vm) => vm.execute(jobs),
        }
    }

    fn new_vm(config: &Config) -> Result<Self> {
        let topo = Topology::new();
        compatible(&topo)?;
        let nb_nodes = get_nb_nodes(&topo, config)?;
        // let vm = VmWrapper::Serial(SerialVM::new(0, config.batch_size));
        // let vm = VmWrapper::Bloom(BloomVM::new(nb_workers, batch_size));

        // let vm = VmWrapper::A(VMa::new(config.address_space_size)?);
        let vm = VmWrapper::B(VMb::new(config.address_space_size, nb_nodes, config.batch_size)?);
        // let vm = VmWrapper::C(VMc::new(config.address_space_size, nb_nodes, config.batch_size)?);

        return Ok(vm);
    }

    fn set_memory(&mut self, value: Word) {
        match self {
            // Self::Basic(vm) => Ok(vec!()),
            // Self::Bloom(vm) => vm.init(jobs).await,
            // Self::Serial(vm) => vm.execute(jobs).await,
            Self::A(vm) => vm.set_memory(value),
            Self::B(vm) => vm.set_memory(value),
            Self::C(vm) => vm.set_memory(value),
        }
    }
}

pub struct ConflictWorkload {
    prep: Vec<Jobs>,
    jobs: Vec<Jobs>,
    // accounts: Vec<TransactionAddress>,
}

impl Workload for ConflictWorkload {
    fn new(config: Config) -> Result<Self> where Self: Sized {
        // TODO Add implement conflict parameter
        let conflict_percentage = 0.5;

        let jobs = batch_with_conflicts(config.batch_size, conflict_percentage);
        print_conflict_rate(&jobs);
        return Ok(Self{
            prep: vec!(),
            jobs: vec!(jobs),
        })
    }

    fn init(&self) -> Vec<Jobs> {
        return self.prep.clone();
    }

    fn load(&mut self) -> Vec<Jobs> {
        return self.jobs.clone();
    }

    fn check(&self) -> bool {
        return true;
    }
}

pub struct ZipfWorkload {
    // prep: Vec<Jobs>,
    // jobs: Vec<Jobs>,
    nb_conflict_transaction: usize
    // accounts: Vec<TransactionAddress>,
}

impl Workload for ZipfWorkload {
    fn new(config: Config) -> Result<Self> where Self: Sized {
        // TODO Add implement conflict parameter
        let conflict_percentage = 0.0;

        if config.address_space_size < 2 * config.batch_size {
            return Err(
                anyhow!("Address space is not large enough. Got {} locations but {} addresses",
                    config.address_space_size, 2 * config.batch_size)
            );
        }

        let mut workload: Jobs = Vec::with_capacity(config.batch_size);
        let nb_conflict_transaction = (config.batch_size as f64 * conflict_percentage).ceil() as usize;
        for _ in 0..nb_conflict_transaction {
            // let tx: Transaction;
            // workload.push(tx);
        }
        for _ in nb_conflict_transaction..config.batch_size {
            // let tx: Transaction;
            // workload.push(tx);
        }

        workload.shuffle(&mut thread_rng());

        return Ok(Self{
            nb_conflict_transaction,
            // prep: vec!(prep),
            // jobs: vec!(jobs),
        })
    }

    fn init(&self) -> Vec<Jobs> {
        return vec!();
    }

    fn load(&mut self) -> Vec<Jobs> {
        return vec!();
    }

    fn check(&self) -> bool {
        return true;
    }
}

pub trait Workload {
    fn new(config: Config) -> Result<Self> where Self: Sized;
    fn init(&self) -> Vec<Jobs>;
    fn load(&mut self) -> Vec<Jobs>;
    // TODO Check that the final state is valid -> need the vm memory as parameter
    fn check(&self) -> bool;
}

pub struct TransactionLoop {
    nb_accounts: usize,
    config: Config,
}
impl Workload for TransactionLoop {
    fn new(config: Config) -> Result<Self> where Self: Sized {
        return Ok(Self{ nb_accounts: 4, config });
    }

    fn init(&self) -> Vec<Jobs> {
        let create_accounts = account_creation_batch(self.nb_accounts, self.nb_accounts, 100);
        return vec!(create_accounts);
    }

    fn load(&mut self) -> Vec<Jobs> {
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
    fn new(config: Config) -> Result<Self> where Self: Sized {
        // TODO replace nb_accounts by the number of workers in the VM?
        return Ok(Self{ nb_accounts: 4, config });
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

    fn load(&mut self) -> Vec<Jobs> {
        let jobs = create_batch_partitioned(self.config.batch_size, self.nb_accounts);
        return vec!(jobs);
    }

    fn check(&self) -> bool {
        return true;
    }
}

pub struct ContentionWorkload {
    contention_generator: ContentionGenerator,
    config: Config,
}
impl Workload for ContentionWorkload {
    fn new(config: Config) -> Result<Self> where Self: Sized {
        let contention_generator = ContentionGenerator::new(
            config.batch_size as u64,
            0.1)?;

        return Ok(Self{ contention_generator, config });
    }

    fn init(&self) -> Vec<Jobs> {
        let nb_accounts =  self.config.batch_size + self.contention_generator.nb_contention_accounts();
        let create_accounts = account_creation_batch(
            self.config.batch_size,
            nb_accounts,
            100
        );

        return vec!(create_accounts);
    }

    fn load(&mut self) -> Vec<Jobs> {
        // let mut g = Box::new(&mut self.contention_generator);
        let jobs = create_batch_from_generator(self.config.batch_size, &mut self.contention_generator);
        // for tx in jobs.iter() {
        //     println!("{:?}", tx);
        // }
        return vec!(jobs);
    }

    fn check(&self) -> bool {
        return true;
    }
}

pub trait Benchmark {
    fn run(config: Config, nb_iter: usize) -> Result<()>;
}

impl<L: 'static> Benchmark for L where L: Workload + Send  {
    // TODO Include VM choice as parameter in config
    fn run(config: Config, nb_iter: usize) -> Result<()>
    {
        // Setup ---------------------------------------------------------------------------------------
        let mut vm = VmWrapper::new_vm(&config)?;

        let mut workload = L::new(config)?;

        let default_balance = 200;

        // Preparation -----------------------------------------------------------------------------
        println!("Preparing workload...");
        let prep_start = Instant::now();
        let prep = workload.init();
        vm.set_memory(default_balance);
        for batch in prep {
            // vm.execute(batch).await?;
            vm.init(batch)?;
        }
        println!("Done. Took {:?}", prep_start.elapsed());

        // Benchmark ---------------------------------------------------------------------------
        println!();
        println!("Benchmarking:");
        let benchmark_start = Instant::now();
        for iter in 0..nb_iter {
            println!("Iteration {}:", iter);
            let load = workload.load();
            let nb_batches = load.len();
            let mut nb_transactions = 0;

            let start = Instant::now();
            for batch in load {
                nb_transactions += batch.len();

                let result = vm.execute(batch)?;
            }
            let duration = start.elapsed();

            if workload.check() {
                // println!("Expected memory total: {}", config.address_space_size as u64 * default_balance);
                vm.set_memory(default_balance);
                print_throughput(nb_batches, nb_transactions, duration);
            } else {
                println!("Incorrect benchmark result");
            }
            println!();
        }
        println!("Total benchmark duration: {:?}", benchmark_start.elapsed());

        Ok(())
    }
}