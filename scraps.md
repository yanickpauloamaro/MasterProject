    fn execute_rec(&mut self,
                         mut results: Vec<ExecutionResult>,
                         mut batch: Jobs,
                         mut backlog: Jobs,
                         mut address_to_worker: Vec<usize>
    ) -> Result<Vec<ExecutionResult>> {
        if batch.is_empty() {
            return Ok(results);
        }

        // Assign jobs to workers ------------------------------------------------------------------
        address_to_worker.fill(NONE);
        let mut tx_to_worker = assign_workers(
            self.nb_workers,
            &batch,
            &mut address_to_worker,
            &mut backlog
        );

        // Execute in parallel ----------------------------------------------------------------
        WorkerC::crossbeam(
            self.nb_workers,
            &mut results,
            &mut batch,
            &mut backlog,
            &mut self.memory,
            &tx_to_worker,
        )?;

        let mut next_backlog = batch;
        next_backlog.clear();

        return self.execute_rec(results, backlog, next_backlog, address_to_worker);
    }

#[async_recursion]
    async fn execute_rec(&mut self,
                         mut results: Vec<ExecutionResult>,
                         mut batch: Jobs,
                         mut backlog: Jobs,
                         mut address_to_worker: Vec<usize>
    ) -> Result<Vec<ExecutionResult>> {
        if batch.is_empty() {
            return Ok(results);
        }

        // Assign jobs to workers ------------------------------------------------------------------
        // TODO Check that the memory accesses are really different?
        address_to_worker.fill(NONE);
        let mut tx_to_worker = assign_workers(
            self.nb_workers,
            &batch,
            &mut address_to_worker,
            &mut backlog
        );

        // Start parallel execution ----------------------------------------------------------------
        let batch_arc = Arc::new(batch);
        let tx_to_worker_arc = Arc::new(tx_to_worker);

        for worker in self.workers.iter_mut() {
            let worker_input = WorkerInput {
                batch: batch_arc.clone(),
                tx_to_worker: tx_to_worker_arc.clone(),
                memory: self.memory.get_shared()
            };

            if let Err(e) = worker.send(worker_input).await {
                println!("VM: Failed to send work to worker {}", worker.index);
            }
        }

        // Collect results -------------------------------------------------------------------------
        for worker in self.workers.iter_mut() {
            let (worker_name, accessed,
                mut worker_output, mut worker_backlog) = worker.receive().await?;
            results.append(&mut worker_output);
            backlog.append(&mut worker_backlog);
        }

        let mut next_backlog = Arc::try_unwrap(batch_arc).unwrap();
        next_backlog.clear();

        return self.execute_rec(results, backlog, next_backlog, address_to_worker).await;
    }
// let mut tx_to_worker = vec![NONE; batch.len()];
// let mut next_worker = 0;
//
// for (index, tx) in batch.iter().enumerate() {
//     let from = tx.from as usize;
//     let to = tx.to as usize;
//
//     let worker_from = address_to_worker[from];
//     let worker_to = address_to_worker[to];
//
//     let assigned = match (worker_from, worker_to) {
//         (NONE, NONE) => {
//             // println!("Neither address is assigned: from={}, to={}", from, to);
//             let worker = next_worker;
//             next_worker = (next_worker + 1) % self.nb_workers;
//             address_to_worker[from] = worker;
//             address_to_worker[to] = worker;
//             worker
//         },
//         (worker, NONE) => {
//             // println!("First address is assigned to {}: from={}, to={}", worker, from, to);
//             address_to_worker[to] = worker;
//             worker
//         },
//         (NONE, worker) => {
//             // println!("Second address is assigned to {}: from={}, to={}", worker, from, to);
//             address_to_worker[from] = worker;
//             worker
//         },
//         (a, b) if a == b => {
//             // println!("Both addresses are assigned to {}: from={}, to={}", a, from, to);
//             a
//         },
//         (a, b) => {
//             // println!("Both addresses are assigned to different workers: from={}->{}, to={}->{}", from, a, to, b);
//             CONFLICT
//         },
//     };
//
//     if assigned == CONFLICT {
//         backlog.push(tx.clone());
//     } else {
//         tx_to_worker[index] = assigned;
//     }
// }

        // // Start parallel execution ----------------------------------------------------------------
        // let mut execution_errors = WorkerC::crossbeam(
        //     self.nb_workers,
        //     &mut results,
        //     &mut batch,
        //     &mut backlog,
        //     &mut self.mut_memory,
        //     &tx_to_worker,
        // );
        // if !execution_errors.is_empty() {
        //     return Err(anyhow!("There where errors during execution: {:?}", execution_errors));
        // }
        // let mut next_backlog = batch;
        // next_backlog.clear();
        //
        // return self.execute_rec(results, backlog, next_backlog, address_to_worker).await;
{

            // let mut execution_errors = vec!();
            // thread::scope(|s| {
            //     let mut memory = self.mut_memory.get_shared();
            //     let mut handles = Vec::with_capacity(self.nb_workers);
            //
            //     for (worker_index, _) in self.workers.iter().enumerate() {
            //         let assignment = batch.iter().zip(tx_to_worker.iter());
            //
            //         handles.push(s.spawn(move |_| {
            //             let mut accessed = vec![0; assignment.len()];
            //             let worker_name = format!("Worker {}", worker_index);
            //
            //             let mut stack: VecDeque<Word> = VecDeque::new();
            //             let mut worker_output = vec!();
            //             let mut worker_backlog = vec!();
            //
            //             for (tx_index, (tx, assigned_worker)) in assignment.clone().enumerate() {
            //                 if *assigned_worker == worker_index {
            //                     stack.clear();
            //                     for instr in tx.instructions.iter() {
            //                         CPU::execute_from_shared(instr, &mut stack, &mut memory);
            //                     }
            //
            //                     let result = ExecutionResult::todo();
            //                     worker_output.push(result);
            //                     accessed[tx_index] = 1;
            //                 }
            //             }
            //
            //             (worker_name, accessed, worker_output, worker_backlog)
            //         }));
            //     }
            //
            //     for handle in handles {
            //         match handle.join() {
            //             Ok((worker_name, accessed, mut worker_output, mut worker_backlog)) => {
            //                 println!("{}: accesses: {:?}", worker_name, accessed);
            //                 results.append(&mut worker_output);
            //                 backlog.append(&mut worker_backlog);
            //             },
            //             Err(e) => {
            //                 execution_errors.push(e);
            //             }
            //         }
            //     }
            // });
        }
// let to_process = batch.len();
        // let mut transactions = Arc::new(batch);
        // let mut results = Vec::with_capacity(batch.len());
        //
        // const CONFLICT: usize = usize::MAX;
        // const DONE: usize = CONFLICT - 1;
        // const NONE: usize = CONFLICT - 2;
        //
        // let mut next_worker = 0;
        // let mut processed = 0;
        //
        // let mut next_transactions = Vec::with_capacity(transactions.len());
        // let mut memory = &mut self.memory;
        //
        // loop {
        //     let mut assigned_workers = vec![NONE; memory.len()];
        //
        //     // Assign addresses to workers
        //     for tx in transactions.iter() {
        //         let worker_from = assigned_workers[tx.from];
        //         let worker_to = assigned_workers[tx.to];
        //
        //         let assigned = match (worker_from, worker_to) {
        //             (NONE, NONE) => {
        //                 let worker = next_worker;
        //                 next_worker = (next_worker + 1) % self.nb_workers;
        //                 assigned_workers[tx.from] = worker;
        //                 assigned_workers[tx.to] = worker;
        //                 worker
        //             },
        //             (NONE, worker) => {
        //                 assigned_workers[tx.from] = worker;
        //                 worker
        //             },
        //             (worker, NONE) => {
        //                 assigned_workers[tx.to] = worker;
        //                 worker
        //             },
        //             (a, b) if a == b => a,
        //             (a, b) => CONFLICT,
        //         };
        //
        //         if assigned == CONFLICT {
        //             next_transactions.push(tx.clone());
        //         } else {
        //             // TODO Need to give access to memory to assigned worker
        //         }
        //     }
        //
        //     // Start workers
        //     let mut handles = Vec::with_capacity(self.nb_workers);
        //     for i in 0..self.nb_workers {
        //         let handle = tokio::spawn(async {
        //             let mut outputs = vec!();
        //             let mut rest = vec!();
        //             for tx in transactions.iter() {
        //                if assigned_workers[tx.from] == i && assigned_workers[tx.to] == i {
        //                    println!("Worker {} is handling {:?}", i, tx);
        //                    let res = ExecutionResult::todo();
        //                    match res {
        //                        ExecutionResult::Output => outputs.push(res),
        //                        ExecutionResult::Transaction(tx) => rest.push(tx)
        //                    }
        //                }
        //            }
        //
        //             (outputs, rest)
        //         });
        //
        //         handles.push(handle);
        //     }
        //
        //     // Collect results
        //     for handle in handles {
        //         let (mut res, mut rest) = handle.await?;
        //         results.append(&mut res);
        //         next_transactions.append(&mut rest);
        //     }
        // }
        //
        // return Ok(results);
{
// let mut handles = Vec::with_capacity(self.nb_workers);
// for i in 0..self.nb_workers {
//
//     // TODO Send to already running worker instead
//     // TODO can create an Arc for the batch?
//     let handle = tokio::spawn(async {
//         let i = i;
//         let mut worker_output = vec!();
//         let mut worker_backlog = vec!();
//         for tx in batch.iter() {
//             if assigned_workers[tx.from as usize] == i &&
//                 assigned_workers[tx.to as usize] == i {
//                 println!("Worker {} is handling {:?}", i, tx);
//                 let res = ExecutionResult::todo();
//                 match res {
//                     ExecutionResult::Output => worker_output.push(res),
//                     ExecutionResult::Transaction(next_tx) => worker_backlog.push(next_tx)
//                 }
//             }
//         }
//
//         (worker_output, worker_backlog)
//     });
//
//     handles.push(handle);
// }

            // // Collect results -------------------------------------------------------------------------
            // for handle in handles {
            //     let (mut worker_output, mut worker_backlog) = handle.await?;
            //     results.append(&mut worker_output);
            //     backlog.append(&mut worker_backlog);
            // }
        }

loop {
// Assign transactions to workers ------------------------------------------------------
for (index, tx) in transactions.iter().enumerate() {
if self.assignment[index] == done {
continue;
}

                if self.bloom_set.has(tx) {
                    self.assignment[index] = conflict;
                } else {
                    self.bloom_set.insert(tx);
                    self.assignment[index] = next_worker;
                    next_worker += 1;
                }
            }

            // Start parallel execution ------------------------------------------------------------
            let assignments = Arc::new(self.assignment.clone());
            for worker in self.tx_jobs.iter() {
                let message = (self.memory.clone(), assignments.clone(), transactions.clone());
            }

            // Synchronize results -----------------------------------------------------------------
            for _ in 0..self.nb_workers {
                // TODO tx created during exec should be added to backlog somehow
                let (mut result, _next_jobs) = self.rx_results.recv().await
                    .ok_or(anyhow!("Unable to receive results from workers"))?;
                processed += result.len();
                self.results.append(&mut result);
            }

            for assignment in self.assignment.iter_mut() {
                if *assignment != done && *assignment != conflict {
                    *assignment = done;
                }
            }

            if processed == to_process {
                break;
            }

            // Clear conflicts for next iteration
            self.bloom_set.clear();
        }

        // Reset the assignment for the next execution
        self.assignment.fill(0);

        let results = self.results
            .drain(..self.results.len())
            .collect();

pub fn print_children(topo: &Topology, obj: &TopologyObject, depth: usize) {
let padding = std::iter::repeat(" ").take(depth).collect::<String>();
println!("{}{}: #{}", padding, obj, obj.os_index());

    for i in 0..obj.arity() {
        print_children(topo, obj.children()[i as usize], depth + 1);
    }
}
fn print_tree_rec(topo: &Topology, obj: &TopologyObject, current_depth: u32, max_depth: u32) {
if current_depth > max_depth {
return;
}
let padding = std::iter::repeat(" ").take(current_depth as usize).collect::<String>();
println!("{}{}: #{}", padding, obj, obj.os_index());
// obj.first_child()
}
pub fn print_tree(topo: &Topology) {
// eprintln!("Hello 1?");
// let padding = std::iter::repeat(" ").take(depth).collect::<String>();
// eprintln!("Hello 2?");
// println!("{}{}: #{}", padding, obj, obj.os_index());
// eprintln!("Hello 3?");
// println!("Arity = {}", obj.arity());
// for i in 0..obj.arity() {
//     eprintln!("Hello {}?", 3 + i);
//     print_children(topo, obj.children()[i as usize], depth + 1);
// }

    // let core_depth = topo.depth_or_below_for_type(&ObjectType::Core).unwrap();
    // let vec = topo.objects_at_depth(1);
    // for o in vec {
    //     print_tree_rec(topo, o, 1, core_depth);
    // }

    // for i in 0..topo.depth() {
    //     println!("*** Objects at level {}", i);
    //
    //     for (idx, object) in topo.objects_at_depth(i).iter().enumerate() {
    //         println!("{}: {}", idx, object);
    //         object.logical_index();
    //         object.parent();
    //         object.
    //     }
    // }
    // let mut all_threads = topo.objects_at_depth(thread_depth);
    // let mut tree = all_threads.pop().unwrap();
    //
    // loop {
    //
    // }
    DumbTree::new().print();
}

struct DumbTree {
topo: Topology,
// layers: Vec<Vec<&'a TopologyObject>>,
}
impl DumbTree {
fn new() -> Self {
Self {
topo: Topology::new()
}
}
fn print(&self) {
let core_depth = self.topo.depth_or_below_for_type(&ObjectType::Core).unwrap();
println!("Core depth = {}", core_depth);
// for core in self.topo.objects_at_depth(core_depth) {
//     println!("Parent is: {}", core.parent().unwrap());
// }
let start_depth = 3;
let root = self.topo.objects_at_depth(start_depth);
// println!("Depth {}", start_depth);
// for el in root.iter() {
//     println!("Calling print_rec on {}", el);
// }
for object in root.iter() {

            self.print_rec(start_depth, object);
        }
    }

    fn print_rec(&self, depth: u32, object: &TopologyObject) {
        let padding = std::iter::repeat("*").take(depth as usize).collect::<String>();
        println!("{}{}: #{}", padding, object, object.os_index());

        let next_depth = depth + 1;
        // println!("next depth = {}", next_depth);
        let children = self.topo.objects_at_depth(next_depth);
        // println!("Depth {}", next_depth);
        // for el in children.iter() {
        //     println!("Calling print_rec on {}", *el);
        // }
        for child in children.iter() {
            // print!("\tChild {}, name {}", child, child.);
            match child.parent() {
                Some(parent) => {
                    println!("Parent is {}, current obj is {}", parent, object);
                    if parent.logical_index() == object.logical_index() {
                        self.print_rec(next_depth, child);
                    }
                },
                None => {
                    println!(" has no parent...");
                    continue
                }
            }
        }
    }
}

async fn execute(backlog: Jobs) -> anyhow::Result<Vec<ExecutionResult>> {
    let mut results = vec!();
    for tx in backlog {

        if self.data.get(&tx.from).is_none() {
            self.data.insert(tx.from, 100);
        }

        if self.data.get(&tx.to).is_none() {
            self.data.insert(tx.to, 100);
        }

        let amount = match self.data.get_mut(&tx.from) {
            Some(sender_balance) if *sender_balance >= tx.amount => {
                *sender_balance -= tx.amount;
                tx.amount
            },
            _ => 0
        };

        self.data
            .get_mut(&tx.to)
            .map_or_else(|| amount, |receiver_balance| {
                *receiver_balance += amount;
                *receiver_balance
            });

        let output = TransactionOutput{ tx };
        let result = ExecutionResult{ output, execution_end: Instant::now()};
        results.push(result);
    }

    return Ok(results);
}


pub async fn benchmark_contract(config: Config, nb_iter: usize) -> Result<()> {

    // Setup ---------------------------------------------------------------------------------------
    let topo = Topology::new();

    // Check compatibility with core pinning
    compatible(&topo)?;

    let nb_nodes = get_nb_nodes(&topo, &config)?;
    // let share = config.address_space_size / nb_nodes;

    // let mut vm = BasicVM::new(nb_nodes, config.batch_size);
    // let mut vm = BloomVM::new(nb_nodes, config.batch_size);
    let mut vm = SerialVM::new(nb_nodes, config.batch_size);
    // vm.prepare().await;

    // Benchmark -----------------------------------------------------------------------------------
    return tokio::spawn(async move {
        println!();
        println!("Benchmarking latency:");

        print!("Creating accounts...");
        let creation_start = Instant::now();
        let account_creation = account_creation_batch(config.batch_size, nb_nodes, 100);
        vm.execute(account_creation).await?;
        println!("Done. Took {:?}", creation_start.elapsed());

        for iter in 0..nb_iter {
            println!("Iteration {}:", iter);
            let batch = transaction_loop(config.batch_size, nb_nodes);

            let start = Instant::now();
            let result = vm.execute(batch).await?;
            let duration = start.elapsed();

            println!("{:?}", vm.data);

            // Computing latency -----------------------------------------------------------
            print_metrics(vec![(result, start, duration)], duration);
            println!();
        }

        Ok(())
    }).await?;
}

pub async fn benchmark(config: Config, nb_iter: usize) -> Result<()> {

    // Setup ---------------------------------------------------------------------------------------
    let topo = Topology::new();

    // Check compatibility with core pinning
    compatible(&topo)?;

    let nb_nodes = get_nb_nodes(&topo, &config)?;
    // let share = config.address_space_size / nb_nodes;

    // let mut vm = BasicVM::new(nb_nodes, config.batch_size);
    let mut vm = BloomVM::new(nb_nodes, config.batch_size);
    // let mut vm = SerialVM::new(nb_nodes, config.batch_size);
    // vm.prepare().await;

    // Benchmark -----------------------------------------------------------------------------------
    return tokio::spawn(async move {
        println!();
        println!("Benchmarking latency:");
        for iter in 0..nb_iter {
            println!("Iteration {}:", iter);
            let batch = create_batch_partitioned(config.batch_size, nb_nodes);

            let start = Instant::now();
            let result = vm.execute(batch).await?;
            let duration = start.elapsed();

            // Computing latency -----------------------------------------------------------
            print_metrics(vec![(result, start, duration)], duration);
            println!();
        }

        Ok(())
    }).await?;
}


type WorkerResult = Either<Vec<Transaction>, ()>;
pub type Log = (u64, Instant, Vec<Instant>); // block id, block creation time

// const BLOCK_SIZE: usize = 64 * 1024;
// const BLOCK_SIZE: usize = 100;
const DEV_RATE: u32 = 100;

pub struct TransactionGenerator {
    /* TODO Check that the generator is fast enough (Throughput > 5 millions/s)
         Otherwise, need to use multiple generators
    */
    pub tx: Sender<Transaction>,
}

impl TransactionGenerator {
    async fn trigger(interval: &mut tokio::time::Interval) {
        async {}.await;
        // interval.tick().await;
    }

    pub fn spawn(self, mut signal: broadcast::Receiver<()>) {
        tokio::spawn(async move {
            println!("<Transaction generator started");

            let duration = Duration::from_secs(1)
                .checked_div(DEV_RATE)
                .unwrap_or(Duration::from_millis(100)); // 10 tx /s
            let mut interval = tokio::time::interval(duration);

            loop {
                tokio::select! {
                    biased;
                    _ = signal.recv() => {
                        println!(">Transaction generator stopped");
                        return;
                    },
                    _ = TransactionGenerator::trigger(&mut interval) => {
                        let tx = Transaction{
                            from: 0,
                            to: 1,
                            amount: 42,
                        };
                        // TODO Generate blocks of transactions to reduce overhead
                        // let block: Vec<Transaction> = (0..batch_size).map(|_| tx).collect();

                        if let Err(e) = self.tx.send(tx).await {
                            eprintln!("Failed to send tx to rate limiter");
                        }
                    }
                }
            }
        });
    }
}

pub struct RateLimiter {
    pub rx: Receiver<Transaction>,
    pub tx: Sender<(Instant, Vec<Transaction>)>,
}

impl RateLimiter {
    pub fn spawn(mut self, mut signal: broadcast::Receiver<()>, batch_size: usize, rate: u32) {
        tokio::spawn(async move {
            println!("<Rate limiter started");

            let mut acc: Vec<Transaction> = Vec::with_capacity(batch_size);
            let duration = Duration::from_secs(1).checked_div(rate)
                .expect("Unable to compute rate limiter interval");

            let loop_start = Instant::now();

            loop {
                tokio::select! {
                    biased;
                    _ = signal.recv() => {
                        println!(">Rate limiter stopped");
                        return;
                    },
                    Some(tx) = self.rx.recv() => {
                        acc.push(tx);

                        if acc.len() >= batch_size {
                            let mut block = Vec::with_capacity(batch_size);
                            mem::swap(&mut block, &mut acc);

                            let creation = Instant::now();
                            if let Err(e) = self.tx.send((creation, block)).await {
                               eprintln!("Failed to send tx to client");
                            }
                        }
                    },
                }
            }
        });
    }
}

pub struct Client {
    pub rx_block: Receiver<(Instant, Vec<Transaction>)>,
    pub tx_jobs: Vec<Sender<(Instant, Vec<Transaction>)>>,
    pub rx_results: Vec<Receiver<WorkerResult>>
}

impl Client {
    pub fn spawn(mut self, mut signal: broadcast::Receiver<()>) {
        tokio::spawn(async move {
            println!("<Client started");
            let mut i = 0;
            let nb_workers = self.tx_jobs.len();

            loop {
                tokio::select! {
                _ = signal.recv() => {
                    println!(">Client stopped");
                    return;
                },

                Some((creation, block)) = self.rx_block.recv() => {
                    // TODO Dispatch properly
                    if let Err(e) = self.tx_jobs[i % nb_workers].send((creation, block)).await {
                        eprintln!("Failed to send jobs to worker");
                    }

                    i += 1;
                }
            }
            }
        });
    }
}

pub struct Worker {
    pub rx_job: Receiver<(Instant, Vec<Transaction>)>,
    pub backlog: Vec<Transaction>,
    pub logs: Vec<Log>,
    pub tx_result: Sender<WorkerResult>,
    pub tx_log: oneshot::Sender<Vec<Log>>,
}

impl Worker {
    pub fn spawn(mut self, mut signal: broadcast::Receiver<()>) {
        tokio::spawn(async move {
            println!("<Worker started");
            let mut i = 0;
            loop {
                tokio::select! {
                _ = signal.recv() => {

                    println!(">Worker stopped");
                    let nb_logs = self.logs.len();
                    if let Err(e) = self.tx_log.send(mem::take(&mut self.logs)) {
                        eprintln!("Failed to send logs to benchmark");
                    }
                    return;
                },

                Some((creation, block)) = self.rx_job.recv() => {
                    // TODO Take conflicts into consideration
                    // let backlog = vec!();
                    let mut log = vec!();
                    for tx in block {
                        // println!("Working on {:?}", tx);
                        // tokio::time::sleep(Duration::from_millis(100)).await;
                        let completion = Instant::now();
                        log.push(completion);
                    }
                    self.logs.push((i, creation, log));
                    i += 1;
                }
            }
            }
        });
    }
}

// Allocate "VM address space" (split per NUMA region?)
// Create nodes (represent NUMA regions?)
// Set memory policy so that memory is allocated locally to each core
// TODO Check how to send transaction to each node
// Create Backlog
// Create dispatcher (will send the transactions to the different regions)
// Create client (will generate transactions)
pub async fn benchmark_rate(config: Config) -> Result<()> {

    // Setup ---------------------------------------------------------------------------------------
    let topo = Topology::new();

    // Check compatibility with core pinning
    compatible(&topo)?;

    // Determine number of cores to use
    let nb_nodes = get_nb_nodes(&topo, &config)?;
    // let share = config.address_space_size / nb_nodes;

    // TODO Move initialisation into spawn and use a broadcast variable to trigger the start of the benchmark
    let (tx_generator, rx_rate) = channel(CHANNEL_CAPACITY);
    let (tx_rate, mut rx_client) = channel(CHANNEL_CAPACITY);
    let generator = TransactionGenerator{
        tx: tx_generator
    };
    let rate_limiter = RateLimiter{
        rx: rx_rate,
        tx: tx_rate
    };

    let mut vm = BasicVM::new(nb_nodes, config.batch_size);
    // vm.prepare().await;

    let (tx_stop, _) = broadcast::channel(1);
    rate_limiter.spawn(tx_stop.subscribe(), config.batch_size, config.rate);
    generator.spawn(tx_stop.subscribe());

    return tokio::spawn(async move {
        println!();
        println!("Benchmarking rate {}s:", config.duration);
        let benchmark_start = Instant::now();
        let timer = tokio::time::sleep(Duration::from_secs(config.duration));
        let mut batch_results = Vec::with_capacity(config.batch_size);
        tokio::pin!(timer);

        loop {
            tokio::select! {
                biased;
                () = &mut timer => {
                    tx_stop.send(()).context("Unable to send stop signal")?;
                    break;
                },
                Some((creation, batch)) = rx_client.recv() => {
                    if benchmark_start.elapsed() > Duration::from_secs(config.duration) {
                        tx_stop.send(()).context("Unable to send stop signal")?;
                        break;
                    }

                    let start = Instant::now();
                    let result = vm.execute(batch).await?;
                    let duration = start.elapsed();

                    batch_results.push((result, start, duration));
                }
            }
        }

        let total_duration = benchmark_start.elapsed();
        println!("Done benchmarking");
        println!();
        // println!("Processed {} batches of {} tx in {:?} s",
        //          batch_processed, config.batch_size, total_duration);
        // println!("Throughput is {} tx/s",
        //          (batch_processed * config.batch_size as u64)/ total_duration.as_secs());
        // println!();

        print_metrics(batch_results, total_duration);
        println!();

        Ok(())
    }).await?;
}


pub async fn benchmark_workload(config: Config) -> Result<()> {

    // Setup ---------------------------------------------------------------------------------------
    let topo = Topology::new();

    // Check compatibility with core pinning
    compatible(&topo)?;

    // Determine number of cores to use
    let nb_nodes = get_nb_nodes(&topo, &config)?;
    // let share = config.address_space_size / nb_nodes;

    let mut workload = Workload::new(
        config.rate as u64,
        config.duration,
        nb_nodes as u64, config.batch_size
    );

    let mut vm = BasicVM::new(nb_nodes, config.batch_size);
    // vm.prepare().await;

    return tokio::spawn(async move {
        println!();
        println!("Benchmarking rate {}s:", config.duration);
        let benchmark_start = Instant::now();
        let timer = tokio::time::sleep(Duration::from_secs(config.duration));
        let mut batch_results = Vec::with_capacity(config.batch_size);
        tokio::pin!(timer);

        loop {
            tokio::select! {
                biased;
                () = &mut timer => {
                    println!("** timer reached **");
                    break;
                },
                // Some((creation, batch)) = rx_client.recv() => {
                batch = workload.next_batch() => {
                    if benchmark_start.elapsed() > Duration::from_secs(config.duration) {
                        println!("** took too long **");
                        break;
                    }
                    if batch.is_none() {
                        println!("Finished benchmark early");
                        break;
                    }

                    let start = Instant::now();
                    let result = vm.execute(batch.unwrap()).await?;
                    let duration = start.elapsed();

                    batch_results.push((result, start, duration));
                }
            }
        }

        let total_duration = benchmark_start.elapsed();
        println!("Done benchmarking");
        println!();

        print_metrics(batch_results, total_duration);
        println!();

        Ok(())
    }).await?;
}


pub struct Workload {
    pub rate: u64,
    pub nb_senders: u64,
    pub batch_size: usize,
    pub backlog: Vec<Batch>,
    // pub rate_limiter: RateLimiter
}

impl Workload {
    pub fn new(rate: u64, duration_s: u64, nb_senders: u64, batch_size: usize) -> Workload {

        let start = Instant::now();
        let nb_batches = rate * duration_s / batch_size as u64;
        let mut batches = Vec::with_capacity(nb_batches as usize);
        print!("Preparing {} batches of {} transactions... ", nb_batches, batch_size);
        std::io::stdout().flush().expect("Failed to flush stdout");

        for _ in 0..nb_batches {
            // TODO use randomness
            let batch = create_batch_partitioned(batch_size, nb_senders as usize);
            batches.push(batch);
        }

        let duration = start.elapsed();
        println!("Done. Took {:?}", duration);
        println!();
        // let limiter = RateLimiter::builder()
        //     .initial(100)
        //     .refill(100)
        //     .max(1000)
        //     .interval(Duration::from_millis(250))
        //     .fair(false)
        //     .build();

        return Workload{
            rate,
            nb_senders,
            batch_size,
            backlog: batches,
            // rate_limiter: limiter
        };
    }

    pub async fn next_batch(&mut self) -> Option<Batch> {
        // TODO add a rate limiter?
        return self.backlog.pop();
    }

    pub fn remaining(&self) -> usize {
        return self.backlog.len();
    }

    pub fn warmup_duration(rate: u64, batch_size: usize, benchmark_duration: u64) -> Result<Duration> {
        let err_batches = anyhow::anyhow!("Unable to compute warmup duration");
        let err_duration = anyhow::anyhow!("Unable to compute warmup duration");

        const SINGLE_BATCH_CREATION_DURATION_MICRO_SECS: Duration = Duration::from_micros(200);
        // let start = Instant::now();
        // let batch = create_batch_partitioned(batch_size, 4);
        // let duration = start.elapsed();

        let nb_batches: u32 = u32::from_u64(rate * benchmark_duration / batch_size as u64)
            .ok_or(err_batches)?;

        let mem_size = mem::size_of::<Transaction>();

        println!("Memory used: {} batches * {}B = {}B",
                 nb_batches, mem_size,
                 nb_batches as usize * mem_size);

        return SINGLE_BATCH_CREATION_DURATION_MICRO_SECS
            .checked_mul(nb_batches)
            .ok_or(err_duration)
            .context("Unable to compute warmup duration");
    }
}
