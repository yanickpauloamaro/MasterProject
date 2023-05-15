Trivial hash (8 schedulers, 24 executors) ==================================================================================

Blake3 hash (8 schedulers, 24 executors) ==================================================================================

Sha256 hash (8 schedulers, 24 executors) ==================================================================================
Sequential {
	8 schedulers, 24 executors {
		PreviousDHashMap(7, 64, 64; 0.2, 0.2, 0.2, 0.2) {
			1.36 ± 0.00 tx/µs
			48.147ms ± 144µs
		}
		PieceDHashMap(7, 64, 64; 0.2, 0.2, 0.2, 0.2) {
			1.34 ± 0.00 tx/µs
			48.833ms ± 72µs
		}
	}
}
ParallelCollect {
	8 schedulers, 24 executors {
		PreviousDHashMap(7, 64, 64; 0.2, 0.2, 0.2, 0.2) {
			0.16 ± 0.00 tx/µs
			401.324ms ± 1.011ms	(scheduling: 394.1ms ± 997µs, execution: 224.854ms ± 1.245ms)
		}
		PieceDHashMap(7, 64, 64; 0.2, 0.2, 0.2, 0.2) {
			0.16 ± 0.00 tx/µs
			407.347ms ± 1.114ms	(scheduling: 393.897ms ± 1.115ms, execution: 229.61ms ± 1.202ms)
		}
	}
}
ParallelImmediate {
	8 schedulers, 24 executors {
		PreviousDHashMap(7, 64, 64; 0.2, 0.2, 0.2, 0.2) {
			0.44 ± 0.00 tx/µs
			150.627ms ± 1.025ms	(scheduling: 149.129ms ± 1.059ms, execution: 134.517ms ± 907µs)
		}
		PieceDHashMap(7, 64, 64; 0.2, 0.2, 0.2, 0.2) {
			0.27 ± 0.00 tx/µs
			246.288ms ± 1.367ms	(scheduling: 143.794ms ± 916µs, execution: 242.279ms ± 1.365ms)
		}
	}
}

Sha512 hash (8 schedulers, 24 executors) ==================================================================================
Sequential {
	8 schedulers, 24 executors {
		PreviousDHashMap(7, 64, 64; 0.2, 0.2, 0.2, 0.2) {
			2.09 ± 0.00 tx/µs
			31.38ms ± 31µs
		}
		PieceDHashMap(7, 64, 64; 0.2, 0.2, 0.2, 0.2) {
			2.01 ± 0.00 tx/µs
			32.667ms ± 17µs
		}
	}
}
ParallelCollect {
	8 schedulers, 24 executors {
		PreviousDHashMap(7, 64, 64; 0.2, 0.2, 0.2, 0.2) {
			0.38 ± 0.00 tx/µs
			174.244ms ± 1.124ms	(scheduling: 165.489ms ± 1.073ms, execution: 70.551ms ± 703µs)
		}
		PieceDHashMap(7, 64, 64; 0.2, 0.2, 0.2, 0.2) {
			0.36 ± 0.00 tx/µs
			180.683ms ± 1.084ms	(scheduling: 165.162ms ± 1.077ms, execution: 79.194ms ± 774µs)
		}
	}
}
ParallelImmediate {
	8 schedulers, 24 executors {
		PreviousDHashMap(7, 64, 64; 0.2, 0.2, 0.2, 0.2) {
			0.82 ± 0.01 tx/µs
			80.18ms ± 962µs	(scheduling: 79.583ms ± 976µs, execution: 52.586ms ± 520µs)
		}
		PieceDHashMap(7, 64, 64; 0.2, 0.2, 0.2, 0.2) {
			0.60 ± 0.00 tx/µs
			109.247ms ± 635µs	(scheduling: 55.577ms ± 342µs, execution: 106.435ms ± 636µs)
		}
	}
}
