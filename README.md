# Microtransactional Blockchain Virtual Machine

For years blockchain systems have promised to change the world by providing a universal platform able to host any online application in a decentralized transparent and safe way. In practice, their scale has been limited by the low throughput of the consensus abstraction and virtual machines they are built upon. With recent research improving consensus performance by multiple orders of magnitude, the virtual machine has become the last obstacle in making blockchains capable of supporting truly internet scale applications. In this context, we present a virtual machine model with relaxed atomicity and isolation guarantees, in which transactions are represented as graphs of micro-transactions, a restricted form of trans- action which declares its memory accesses before execution. We compare two scheduling-based implementations of this model using different scheduling algorithms and evaluate them on three applications: a parallel hashmap, a banking app and a synthetic application. Compared to a sequential baseline, we show that using a basic algorithm performs well on all applications but make the VM quite sensitive to contention. On the flip side, using a more advanced algorithm makes the VM more resilient to contention but provide poor performance on applications with random memory accesses. Both implementations achieve close to linear improvement with up to 16 cores on the synthetic workload.

## Description
This repository is part of my master thesis at EPFL on parallel blockchain virtual machines. It contains two prototype implementations written in Rust as well as three test applications: a distributed hashmap, a banking app and a synthetic, high computation application.

The benchmark results and the graphs used in the report can be found under `report/graphs`.
The graphs are written in javascript using [Apache ECharts](https://echarts.apache.org/en/index.html) and interactive versions of the graphs can be explored using Echarts' [online editor](https://echarts.apache.org/examples/en/editor.html?c=line-simple) by simply copy-pasting the graph's code.

## Quick Start
To run the benchmark yourself clone the repository and make sure to have both Rust and gcc installed.
Then you simply run `cargo run --release` to run the benchmark.
The configuration for the benchmark can be found in `benchmark_config.json` and the important parameters are explained below (the other ones are only used for testing):
- `vm_types`: the list of VM variants to benchmark. The possible values are: `Sequential`, `BasicPrototype` and `AdvancedPrototype`.
- `nb_schedulers` and `nb_executors`: The number of threads to use for scheduling and executing transactions respectively. They are provided as a list to more easily benchmark multiple configurations. NB: Each thread is pinned to a unique logical core so make sure to have enough logical cores for all scheduling and execution threads.
- `batch_sizes`: The number of transactions to process per iteration. Default value is 65536.
- `workloads`: The list of workloads to benchmark. The accepted values are presented below.
- `repetitions`: The number of iterations of the benchmark. The result will be the mean of all the repetitions.
- `seed`: The seed used to generate the workloads. Can be omitted to produce a random seed.

### Workloads
- `Transfer(<conflict>)`: workload for the banking application where `<conflict>` controls the percentage of conflict (0.0 < `<conflict>` < 1.0).
- `Fibonacci(<n>)`: workload for the synthetic application where `<n>` is the fibonacci number to compute.
- `Hashmap(<value_size>, <bucket_capacity>, <nb_buckets>; <get>, <insert>, <remove>, <contains>)`: workload for the hashmap application.
    - `<value_size>`: The size of a value, in words (1 word = 8B). A map entry has one more byte for the key.
    - `<bucket_capacity>`: The capacity of the buckets, in entries.
    - `<nb_buckets>`: The initial number of buckets of the hashmap.
    - `<get>`: The percentage of `get` operations in the workload.
    - `<insert>`: The percentage of `insert` operations in the workload.
    - `<remove>`: The percentage of `remove` operations in the workload.
    - `<contains>`: The percentage of `contain` operations in the workload.
    - NB: If they do not sum to 1.0, they will be generated proportionally.