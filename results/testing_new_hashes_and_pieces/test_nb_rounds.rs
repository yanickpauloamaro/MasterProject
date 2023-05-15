Trivial hash (8 schedulers, 1 executors) ==================================================================================
ParallelCollect
    Previous:   Took 105 execution rounds
    Piece:      Took 113 execution rounds
ParallelImmediate
    Previous:   Took 292 execution rounds
    Piece:      Took 1138 execution rounds

Blake3 hash (8 schedulers, 1 executors) ==================================================================================
ParallelCollect
    Previous:   Took 100 execution rounds
    Piece:      Took 108 execution rounds
ParallelImmediate
    Previous:   Took 293 execution rounds
    Piece:      Took 1125 execution rounds <<<<<<<<<<<<<<<< why this increase? why doesn't it appear in the previous version?
/*
ParallelCollect
    Previous:   Took 101 execution rounds
    Piece:      Took 109 execution rounds
ParallelImmediate
    Previous:   Took 286 execution rounds
    Piece:      Took 1129 execution rounds
*/
Sha256 hash (8 schedulers, 1 executors) ==================================================================================
ParallelCollect
    Previous:   Took 12797 execution rounds
    Piece:      Took 12805 execution rounds
ParallelImmediate
    Previous:   Took 13085 execution rounds
    Piece:      Took 14435 execution rounds
/*
Modified hash (sum of &[u8] instead of truncate)
ParallelCollect
    Previous:   Took 391 execution rounds
    Piece:      Took 399 execution rounds
ParallelImmediate
    Previous:   Took 611 execution rounds
    Piece:      Took 1604 execution rounds

Modified hash (sum of u8 instead of truncate)
ParallelCollect
    Previous:   Took 145 execution rounds
    Piece:      Took 153 execution rounds
ParallelImmediate
    Previous:   Took 345 execution rounds
    Piece:      Took 1230 execution rounds
*/

Sha512 hash (8 schedulers, 1 executors) ==================================================================================
ParallelCollect
    Previous:   Took 102 execution rounds
    Piece:      Took 110 execution rounds
ParallelImmediate
    Previous:   Took 296 execution rounds
    Piece:      Took 1128 execution rounds
/*
ParallelCollect
    Previous:   Took 102 execution rounds
    Piece:      Took 110 execution rounds
ParallelImmediate
    Previous:   Took 295 execution rounds
    Piece:      Took 1113 execution rounds
*/