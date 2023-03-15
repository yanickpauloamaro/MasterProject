use std::cell::RefCell;
use std::ops::Deref;
use std::rc::Rc;
use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput, BenchmarkId, BatchSize, BenchmarkGroup};
use testbench::utils::create_batch_partitioned;
use testbench::vm_implementation::{VMa, VMb};
use testbench::wip::Executor;
use criterion::async_executor::FuturesExecutor;
use tokio::runtime::{Handle, Runtime};
use testbench::transaction::Transaction;
use testbench::worker_implementation::{WorkerBStd, WorkerBTokio};
use anyhow::Result;
use std::borrow::Borrow;
use criterion::measurement::WallTime;

criterion_group!(benches, benchmark_vma);
criterion_main!(benches);

//region Benchmarks ================================================================================
pub fn benchmark_vma(c: &mut Criterion) {
    let factory = |memory_size: usize, nb_cores: usize, batch_size: usize| {
        VMa::new(memory_size).unwrap()
    };

    // benchmark_core_scaling(c, factory, "serial");
    benchmark_batch_size_scaling(c, factory, "serial");
    // benchmark_conflict_rate_scaling(c, factory, "serial");
}

pub fn benchmark_vmb_tokio(c: &mut Criterion) {
    let factory = |memory_size: usize, nb_cores: usize, batch_size: usize| {
        VMb::<WorkerBTokio>::new( memory_size, nb_cores, batch_size).unwrap()
    };

    let rt = Runtime::new().unwrap();
    let _guard = rt.enter();

    benchmark_core_scaling(c, factory, "parallel_tokio");
    // benchmark_batch_size_scaling(c, factory, "parallel_tokio");
    // benchmark_conflict_rate_scaling(c, factory, "parallel_tokio");
}

pub fn benchmark_vmb_std(c: &mut Criterion) {
    let factory = |memory_size: usize, nb_cores: usize, batch_size: usize| {
        VMb::<WorkerBStd>::new(memory_size, nb_cores, batch_size).unwrap()
    };

    benchmark_core_scaling(c, factory, "parallel_std");
    // benchmark_batch_size_scaling(c, factory, "parallel_std");
    // benchmark_conflict_rate_scaling(c, factory, "parallel_std");
}
//endregion

//region Benchmark types ===========================================================================
fn benchmark_core_scaling<V: Executor + Sized>(
    c: &mut Criterion, factory: fn(usize, usize, usize)->V, name: &str
)
{
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
            factory,
            nb_cores, memory_size, batch_size, conflict_rate,
            nb_cores
        );
    }
    group.finish();
}

fn benchmark_batch_size_scaling<V: Executor + Sized>(
    c: &mut Criterion, factory: fn(usize, usize, usize)->V, name: &str
)
{
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
            factory,
            nb_cores, memory_size, batch_size, conflict_rate,
            batch_size
        );
    }
    group.finish();
}

fn benchmark_conflict_rate_scaling<V: Executor + Sized>(
    c: &mut Criterion, factory: fn(usize, usize, usize)->V, name: &str
)
{
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
            factory,
            nb_cores, memory_size, batch_size, conflict_rate,
            conflict_rate
        );
    }
    group.finish();
}
//endregion

//region Benchmark tool ============================================================================
fn bench_with_parameter<P: ::std::fmt::Display, V: Executor + Sized>(
    group: &mut BenchmarkGroup<WallTime>,
    factory: fn(usize, usize, usize) -> V,
    nb_core: usize, memory_size: usize, batch_size: usize, conflict_rate: f64,
    param: P
) {

    group.throughput(Throughput::Elements(batch_size as u64));

    // One time setup: ---------------------------------------------------------------------
    let vm = RefCell::new(
        factory(memory_size, nb_core, batch_size)
    );

    group.bench_function(
        BenchmarkId::from_parameter(param), |b| {
            // Per sample setup (might be multiple iteration) ------------------------------
            // Nothing
            b.iter_batched(
                || {
                    // Input creation (per iteration?)
                    // TODO vm.clear()?
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

