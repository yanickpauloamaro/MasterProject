Trivial hash (8 schedulers, 24 executors) ==================================================================================
Sequential {
	PreviousDHashMap(7, 64, 1024; 0.2, 0.2, 0.2, 0.2) {
		6.105ms ± 9µs
	}
	PieceDHashMap(7, 64, 1024; 0.2, 0.2, 0.2, 0.2) {
		6.865ms ± 5µs
	}
}
ParallelCollect {
	PreviousDHashMap(7, 64, 1024; 0.2, 0.2, 0.2, 0.2) {
		18.559ms ± 219µs	(scheduling: 11.999ms ± 178µs, execution: 11.418ms ± 238µs)
	}
	PieceDHashMap(7, 64, 1024; 0.2, 0.2, 0.2, 0.2) {
		25.509ms ± 199µs	(scheduling: 12.357ms ± 193µs, execution: 17.791ms ± 197µs)
	}
}
ParallelImmediate {
	PreviousDHashMap(7, 64, 1024; 0.2, 0.2, 0.2, 0.2) {
		15.361ms ± 197µs	(scheduling: 9.257ms ± 91µs, execution: 13.211ms ± 189µs)
	}
	PieceDHashMap(7, 64, 1024; 0.2, 0.2, 0.2, 0.2) {
		66.127ms ± 519µs	(scheduling: 16.223ms ± 40µs, execution: 65.017ms ± 518µs)
	}
}

Blake3 hash (8 schedulers, 24 executors) ==================================================================================
Sequential {
	PreviousDHashMap(7, 64, 1024; 0.2, 0.2, 0.2, 0.2) {
		9.409ms ± 9µs
	}
	PieceDHashMap(7, 64, 1024; 0.2, 0.2, 0.2, 0.2) {
		10.725ms ± 5µs
	}
}
ParallelCollect {
	PreviousDHashMap(7, 64, 1024; 0.2, 0.2, 0.2, 0.2) {
		18.801ms ± 219µs	(scheduling: 12.204ms ± 204µs, execution: 11.418ms ± 182µs)
	}
	PieceDHashMap(7, 64, 1024; 0.2, 0.2, 0.2, 0.2) {
		25.647ms ± 223µs	(scheduling: 11.98ms ± 186µs, execution: 18.201ms ± 222µs)
	}
}
ParallelImmediate {
	PreviousDHashMap(7, 64, 1024; 0.2, 0.2, 0.2, 0.2) {
		15.792ms ± 195µs	(scheduling: 9.533ms ± 105µs, execution: 13.683ms ± 187µs)
	}
	PieceDHashMap(7, 64, 1024; 0.2, 0.2, 0.2, 0.2) {
		65.47ms ± 560µs	(scheduling: 16.222ms ± 32µs, execution: 64.364ms ± 560µs)
	}
}

Sha256 hash (8 schedulers, 24 executors) ==================================================================================
Sequential {
	PreviousDHashMap(7, 64, 1024; 0.2, 0.2, 0.2, 0.2) {
		79.894ms ± 169µs
	}
	PieceDHashMap(7, 64, 1024; 0.2, 0.2, 0.2, 0.2) {
		78.161ms ± 129µs
	}
}
ParallelCollect {
	PreviousDHashMap(7, 64, 1024; 0.2, 0.2, 0.2, 0.2) {
		200.03ms ± 959µs	(scheduling: 192.662ms ± 953µs, execution: 133.016ms ± 1.07ms)
	}
	PieceDHashMap(7, 64, 1024; 0.2, 0.2, 0.2, 0.2) {
		208.973ms ± 961µs	(scheduling: 195.008ms ± 948µs, execution: 139.707ms ± 1.053ms)
	}
}
ParallelImmediate {
	PreviousDHashMap(7, 64, 1024; 0.2, 0.2, 0.2, 0.2) {
		113.372ms ± 697µs	(scheduling: 103.04ms ± 905µs, execution: 108.432ms ± 701µs)
	}
	PieceDHashMap(7, 64, 1024; 0.2, 0.2, 0.2, 0.2) {
		150.269ms ± 667µs	(scheduling: 61.635ms ± 354µs, execution: 147.161ms ± 667µs)
	}
}
/*
Sequential {
    PreviousDHashMap(7, 64, 1024; 0.2, 0.2, 0.2, 0.2) {
        22.188ms ± 10µs
    }
    PieceDHashMap(7, 64, 1024; 0.2, 0.2, 0.2, 0.2) {
        23.682ms ± 9µs
    }
}
ParallelCollect {
    PreviousDHashMap(7, 64, 1024; 0.2, 0.2, 0.2, 0.2) {
        22.318ms ± 282µs	(scheduling: 13.852ms ± 200µs, execution: 14.554ms ± 234µs)
    }
    PieceDHashMap(7, 64, 1024; 0.2, 0.2, 0.2, 0.2) {
        29.156ms ± 234µs	(scheduling: 14.527ms ± 183µs, execution: 20.994ms ± 272µs)
    }
}
ParallelImmediate {
    PreviousDHashMap(7, 64, 1024; 0.2, 0.2, 0.2, 0.2) {
        16.858ms ± 138µs	(scheduling: 11.235ms ± 109µs, execution: 14.751ms ± 128µs)
    }
    PieceDHashMap(7, 64, 1024; 0.2, 0.2, 0.2, 0.2) {
        60.376ms ± 1.136ms	(scheduling: 16.894ms ± 36µs, execution: 59.301ms ± 1.132ms) <<<<<<<<<<<<<<<<<< caused by too many rounds
    }
}

*/
Sha512 hash (8 schedulers, 24 executors) ==================================================================================
Sequential {
	PreviousDHashMap(7, 64, 1024; 0.2, 0.2, 0.2, 0.2) {
		24.02ms ± 10µs
	}
	PieceDHashMap(7, 64, 1024; 0.2, 0.2, 0.2, 0.2) {
		25.304ms ± 6µs
	}
}
ParallelCollect {
	PreviousDHashMap(7, 64, 1024; 0.2, 0.2, 0.2, 0.2) {
		22.388ms ± 239µs	(scheduling: 13.199ms ± 164µs, execution: 14.687ms ± 250µs)
	}
	PieceDHashMap(7, 64, 1024; 0.2, 0.2, 0.2, 0.2) {
		28.85ms ± 189µs	(scheduling: 12.366ms ± 179µs, execution: 21.507ms ± 245µs)
	}
}
ParallelImmediate {
	PreviousDHashMap(7, 64, 1024; 0.2, 0.2, 0.2, 0.2) {
		17.305ms ± 204µs	(scheduling: 10.562ms ± 127µs, execution: 15.159ms ± 194µs)
	}
	PieceDHashMap(7, 64, 1024; 0.2, 0.2, 0.2, 0.2) {
		67.748ms ± 570µs	(scheduling: 16.572ms ± 41µs, execution: 66.653ms ± 569µs)
	}
}
