use std::cell::RefCell;
use std::ops::Deref;
use std::rc::Rc;
use criterion::{BatchSize, BenchmarkGroup, BenchmarkId, black_box, Criterion, criterion_group, criterion_main, Throughput};
use testbench::utils::create_batch_partitioned;
use testbench::vm_implementation::{VMa, VMb, VMc, VmFactory, VmType};
use testbench::wip::Executor;
use criterion::async_executor::FuturesExecutor;
use tokio::runtime::{Handle, Runtime};
use testbench::transaction::Transaction;
use testbench::worker_implementation::{WorkerBStd, WorkerBTokio};
use anyhow::Result;
use std::borrow::Borrow;
use criterion::measurement::WallTime;

criterion_group!(benches, benchmark_vm_scaling);
criterion_main!(benches);

pub fn manual_benchmark(c: &mut Criterion) {
    let batch_size = 128;
    let memory_size = 2 * batch_size;
    let nb_cores = 4;
    let conflict_rate = 0.0;

    let vm_type = VmType::A;
    // let vm_type = VmType::BTokio;
    // let vm_type = VmType::BStd;
    // let vm_type = VmType::C;

    let mut group = c.benchmark_group(
        format!("manual")
    );

    let rt = Runtime::new().unwrap();
    let _guard = rt.enter();
    bench_with_parameter(
        &mut group,
        &vm_type,
        nb_cores, memory_size, batch_size, conflict_rate,
        nb_cores
    );

    group.finish();
}

//region Benchmarks ================================================================================
pub fn benchmark_vma(c: &mut Criterion) {

    // let factory = Box::new(FactoryA);//VMa::factory();
    let vm_type = VmType::A;

    // benchmark_core_scaling(c, &vm_type, "serial");
    benchmark_batch_size_scaling(c, &vm_type, "serial");
    // benchmark_conflict_rate_scaling(c, &vm_type, "serial");
}

pub fn benchmark_vmb_tokio(c: &mut Criterion) {
    // let factory = Box::new(FactoryBTokio);//VMb::<WorkerBTokio>::factory();
    let vm_type = VmType::BTokio;

    let rt = Runtime::new().unwrap();
    let _guard = rt.enter();

    benchmark_core_scaling(c, &vm_type, "parallel_tokio");
    // benchmark_batch_size_scaling(c, &vm_type, "parallel_tokio");
    // benchmark_conflict_rate_scaling(c, &vm_type, "parallel_tokio");
}

pub fn benchmark_vmb_std(c: &mut Criterion) {
    let vm_type = VmType::BStd;

    benchmark_core_scaling(c, &vm_type, "parallel_std");
    // benchmark_batch_size_scaling(c, &vm_type, "parallel_std");
    // benchmark_conflict_rate_scaling(c, &vm_type, "parallel_std");
}

pub fn benchmark_vmc(c: &mut Criterion) {
    let vm_type = VmType::C;

    benchmark_core_scaling(c, &vm_type, "parallel_crossbeam");
    // benchmark_batch_size_scaling(c, &vm_type, "parallel_std");
    // benchmark_conflict_rate_scaling(c, &vm_type, "parallel_std");
}
//endregion

//region Benchmark types ===========================================================================
fn benchmark_vm_scaling(c: &mut Criterion) {
    let batch_size = 128;
    let memory_size = 2 * batch_size;
    let nb_cores = 4;
    let conflict_rate = 0.0;

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
    let batch_size = 128;
    let memory_size = 2 * batch_size;
    let cores: Vec<usize> = vec![1, 2]; // TODO vec![1, 2, 4, 8, 16, 32];
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
    let batch_size = 128;
    let memory_size = 2 * batch_size;
    let nb_cores = 4;
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
        // factory.new(memory_size, nb_core, batch_size)
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
                    const PARTITION_SIZE: usize = 4;
                    create_batch_partitioned(batch_size, PARTITION_SIZE)
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

