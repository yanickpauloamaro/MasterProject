use std::cell::RefCell;
use std::ops::Deref;
use std::rc::Rc;
use criterion::{BatchSize, BenchmarkGroup, BenchmarkId, black_box, Criterion, criterion_group, criterion_main, PlotConfiguration, Throughput};
use testbench::utils::{batch_with_conflicts, batch_partitioned};
use testbench::vm_implementation::{VMa, VMb, VMc, VmFactory, VmType};
use testbench::vm::Executor;
use criterion::async_executor::FuturesExecutor;
use tokio::runtime::{Handle, Runtime};
use testbench::transaction::Transaction;
use testbench::worker_implementation::{WorkerBStd, WorkerBTokio};
use anyhow::{Context, Result};
use std::borrow::Borrow;
use criterion::measurement::WallTime;
use testbench::config::{BenchmarkConfig, ConfigFile};

// criterion_group!(benches, benchmark_vma);
// criterion_group!(benches, benchmark_vmc);
// criterion_group!(benches, benchmark_vmb_std);
// criterion_group!(benches, benchmark_vmb_tokio);

// criterion_group!(benches, benchmark_aws_cores, benchmark_aws_conflict);
criterion_group!(benches, benchmark_from_config);
criterion_main!(benches);

pub fn benchmark_aws_cores(c: &mut Criterion) {
    let mut group = c.benchmark_group(
        "aws_cores"
    );

    let vm_types = vec![
        // VmType::A,
        VmType::BTokio,
        VmType::BStd,
        VmType::C
    ];

    let batch_size = 65536;
    let memory_size = 2 * batch_size;
    let nb_cores = [1, 2, 4, 8, 16, 32];

    let conflict_rate = 0.0;

    let vma = VmType::A;
    let param_str = format!("{}-{}_cores-{}_conflict_rate", vma.name(), 1, conflict_rate);
    bench_with_parameter(
        &mut group,
        &vma,
        1, memory_size, batch_size, conflict_rate,
        param_str
    );

    for vm_type in vm_types.iter() {
        for nb_core in nb_cores.iter() {
            let param_str = format!("{}-{}_cores-{}_conflict_rate", vm_type.name(), nb_core, conflict_rate);
            bench_with_parameter(
                &mut group,
                &vm_type,
                *nb_core, memory_size, batch_size, conflict_rate,
                param_str
            );
        }
    }

    group.finish();
}

pub fn benchmark_aws_conflict(c: &mut Criterion) {
    let mut group = c.benchmark_group(
        "aws_conflicts"
    );

    let vm_types = vec![
        VmType::A,
        VmType::BTokio,
        VmType::BStd,
        VmType::C
    ];

    let batch_size = 65536;
    let memory_size = 2 * batch_size;
    let nb_core = 32;

    let conflict_rates = vec![0.0, 0.01, 0.1, 0.5];

    for vm_type in vm_types.iter() {
        for conflict_rate in conflict_rates.iter() {
            let param_str = format!("{}-{}_cores-{}_conflict_rate", vm_type.name(), nb_core, conflict_rate);
            bench_with_parameter(
                &mut group,
                &vm_type,
                nb_core, memory_size, batch_size, *conflict_rate,
                param_str
            );
        }
    }

    group.finish();
}

pub fn benchmark_from_config(c: &mut Criterion) -> Result<()> {

    let mut group = c.benchmark_group(
        "manual"
    );

    let config = BenchmarkConfig::new("benchmark_config.json")
        .context("Unable to create benchmark config")?;

    for vm_type in config.vm_types.iter() {
        for nb_core in config.nb_cores.iter() {
            for batch_size in config.batch_sizes.iter() {
                let memory_size= 2 * batch_size;
                for conflict_rate in config.conflict_rates.iter() {
                    let param_str = format!("{}-{}-{}-{}", vm_type.name() ,nb_core, batch_size, conflict_rate);
                    bench_with_parameter(
                        &mut group,
                        vm_type,
                        *nb_core, memory_size, *batch_size, *conflict_rate,
                        param_str
                    );
                }
            }
        }
    }
    group.finish();
    Ok(())
}

//region Benchmarks ================================================================================
pub fn benchmark_vma(c: &mut Criterion) {

    // let factory = Box::new(FactoryA);//VMa::factory();
    let vm_type = VmType::A;

    // benchmark_core_scaling(c, &vm_type, "serial");
    // benchmark_batch_size_scaling(c, &vm_type, "serial");
    benchmark_conflict_rate_scaling(c, &vm_type, "serial");
}

pub fn benchmark_vmb_tokio(c: &mut Criterion) {
    // let factory = Box::new(FactoryBTokio);//VMb::<WorkerBTokio>::factory();
    let vm_type = VmType::BTokio;

    // let rt = Runtime::new().unwrap();
    // let _guard = rt.enter();

    benchmark_core_scaling(c, &vm_type, "parallel_tokio");
    // benchmark_batch_size_scaling(c, &vm_type, "parallel_tokio");
    benchmark_conflict_rate_scaling(c, &vm_type, "parallel_tokio");
}

pub fn benchmark_vmb_std(c: &mut Criterion) {
    let vm_type = VmType::BStd;

    benchmark_core_scaling(c, &vm_type, "parallel_std");
    // benchmark_batch_size_scaling(c, &vm_type, "parallel_std");
    benchmark_conflict_rate_scaling(c, &vm_type, "parallel_std");
}

pub fn benchmark_vmc(c: &mut Criterion) {
    let vm_type = VmType::C;

    benchmark_core_scaling(c, &vm_type, "parallel_crossbeam");
    // benchmark_batch_size_scaling(c, &vm_type, "parallel_std");
    benchmark_conflict_rate_scaling(c, &vm_type, "parallel_std");
}
//endregion

//region Benchmark types ===========================================================================
fn benchmark_vm_scaling(c: &mut Criterion) {
    let batch_size = 65536;
    let memory_size = 2 * batch_size;
    let nb_cores = 4;
    let conflict_rate = 0.1;

    let vm_types = vec![
        VmType::A,
        VmType::BTokio,
        VmType::BStd,
        VmType::C
    ];

    let mut group = c.benchmark_group(
        "vm_comparison"
    );

    for vm_type in vm_types.into_iter() {
        bench_with_parameter(
            &mut group,
            &vm_type,
            nb_cores, memory_size, batch_size, conflict_rate,
            vm_type.name()
        );
    }
    group.finish();
}

fn benchmark_core_scaling(c: &mut Criterion, vm_type: &VmType, name: &str) {
    let batch_size = 65536;
    let memory_size = 2 * batch_size;
    // let cores: Vec<usize> = vec![1, 2, 4]; // TODO vec![1, 2, 4, 8, 16, 32];
    let cores: Vec<usize> = vec![1, 2, 4, 8, 16, 32];
    let conflict_rate = 0.0;

    let mut group = c.benchmark_group(
        format!("{}_core_scaling", name)
    );

    for nb_cores in cores.into_iter() {
        bench_with_parameter(
            &mut group,
            vm_type,
            nb_cores, memory_size, batch_size, conflict_rate,
            nb_cores
        );
    }
    group.finish();
}

fn benchmark_batch_size_scaling(c: &mut Criterion, vm_type: &VmType, name: &str) {
    let batch_sizes: Vec<usize> = vec![128, 256, 512];  // TODO Use much larger sizes
    let nb_cores = 4;
    let conflict_rate = 0.0;

    let mut group = c.benchmark_group(
        format!("{}_batch_scaling", name)
    );

    for batch_size in batch_sizes.into_iter() {
        let memory_size = 2 * batch_size;
        bench_with_parameter(
            &mut group,
            vm_type,
            nb_cores, memory_size, batch_size, conflict_rate,
            batch_size
        );
    }
    group.finish();
}

fn benchmark_conflict_rate_scaling(c: &mut Criterion, vm_type: &VmType, name: &str) {
    let batch_size = 65536;
    let memory_size = 2 * batch_size;
    let nb_cores = 32;
    let conflict_rates = vec![0.0, 0.01, 0.1, 0.5];

    let mut group = c.benchmark_group(
        format!("{}_conflict_scaling", name)
    );

    for conflict_rate in conflict_rates.into_iter() {
        bench_with_parameter(
            &mut group,
            vm_type,
            nb_cores, memory_size, batch_size, conflict_rate,
            conflict_rate
        );
    }
    group.finish();
}
//endregion

//region Benchmark tool ============================================================================
fn bench_with_parameter<P: ::std::fmt::Display>(
    group: &mut BenchmarkGroup<WallTime>,
    vm_type: &VmType,
    nb_core: usize, memory_size: usize, batch_size: usize, conflict_rate: f64,
    param: P
)
{

    group.throughput(Throughput::Elements(batch_size as u64));

    // One time setup: ---------------------------------------------------------------------
    let vm = RefCell::new(
        VmFactory::new_vm(vm_type, memory_size, nb_core, batch_size)
    );

    group.bench_function(
        BenchmarkId::from_parameter(param), |b| {
            // Per sample setup (might be multiple iteration) ------------------------------
            // Nothing
            b.iter_batched(
                || {
                    // Input creation (per iteration?)
                    vm.borrow_mut().set_memory(100);

                    // TODO use conflict rate to generate input
                    batch_with_conflicts(batch_size, conflict_rate)
                },
                |input| {
                    // Measured code (includes the time needed to drop the result)
                    let _ = vm.borrow_mut().execute(input);
                    ()
                },
                BatchSize::SmallInput
            )
        }
    );
}
//endregion

