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

