let data = {
    datasetId: 'data',
    source: [
  // Transfer(0.0): =======================================================================
  {workload: 'Transfer(0.0)', scheduling: 'basic', size: 65537, parallelism: 1.000, latency: 1822, iter: 1 },
  {workload: 'Transfer(0.0)', scheduling: 'basic', size: 32769, parallelism: 1.000, latency: 812, iter: 1 },
  {workload: 'Transfer(0.0)', scheduling: 'basic', size: 16385, parallelism: 1.000, latency: 380, iter: 1 },
  {workload: 'Transfer(0.0)', scheduling: 'basic', size: 8193, parallelism: 1.000, latency: 177, iter: 1 },
  {workload: 'Transfer(0.0)', scheduling: 'basic', size: 4097, parallelism: 1.000, latency: 80, iter: 1 },
  {workload: 'Transfer(0.0)', scheduling: 'basic', size: 2731, parallelism: 1.000, latency: 51, iter: 1 },
  {workload: 'Transfer(0.0)', scheduling: 'basic', size: 2049, parallelism: 1.000, latency: 38, iter: 1 },
  
  {workload: 'Transfer(0.0)', scheduling: 'advanced(4)', size: 65537, parallelism: 1.000, latency: 3707, iter: 1 },
  {workload: 'Transfer(0.0)', scheduling: 'advanced(4)', size: 32769, parallelism: 1.000, latency: 1654, iter: 1 },
  {workload: 'Transfer(0.0)', scheduling: 'advanced(4)', size: 16385, parallelism: 1.000, latency: 798, iter: 1 },
  {workload: 'Transfer(0.0)', scheduling: 'advanced(4)', size: 8193, parallelism: 1.000, latency: 390, iter: 1 },
  {workload: 'Transfer(0.0)', scheduling: 'advanced(4)', size: 4097, parallelism: 1.000, latency: 179, iter: 1 },
  {workload: 'Transfer(0.0)', scheduling: 'advanced(4)', size: 2731, parallelism: 1.000, latency: 97, iter: 1 },
  {workload: 'Transfer(0.0)', scheduling: 'advanced(4)', size: 2049, parallelism: 1.000, latency: 71, iter: 1 },
  {workload: 'Transfer(0.0)', scheduling: 'advanced(8)', size: 65537, parallelism: 1.000, latency: 3696, iter: 1 },
  {workload: 'Transfer(0.0)', scheduling: 'advanced(8)', size: 32769, parallelism: 1.000, latency: 1660, iter: 1 },
  {workload: 'Transfer(0.0)', scheduling: 'advanced(8)', size: 16385, parallelism: 1.000, latency: 798, iter: 1 },
  {workload: 'Transfer(0.0)', scheduling: 'advanced(8)', size: 8193, parallelism: 1.000, latency: 390, iter: 1 },
  {workload: 'Transfer(0.0)', scheduling: 'advanced(8)', size: 4097, parallelism: 1.000, latency: 178, iter: 1 },
  {workload: 'Transfer(0.0)', scheduling: 'advanced(8)', size: 2731, parallelism: 1.000, latency: 99, iter: 1 },
  {workload: 'Transfer(0.0)', scheduling: 'advanced(8)', size: 2049, parallelism: 1.000, latency: 71, iter: 1 },
  {workload: 'Transfer(0.0)', scheduling: 'advanced(16)', size: 65537, parallelism: 1.000, latency: 3717, iter: 1 },
  {workload: 'Transfer(0.0)', scheduling: 'advanced(16)', size: 32769, parallelism: 1.000, latency: 1658, iter: 1 },
  {workload: 'Transfer(0.0)', scheduling: 'advanced(16)', size: 16385, parallelism: 1.000, latency: 799, iter: 1 },
  {workload: 'Transfer(0.0)', scheduling: 'advanced(16)', size: 8193, parallelism: 1.000, latency: 390, iter: 1 },
  {workload: 'Transfer(0.0)', scheduling: 'advanced(16)', size: 4097, parallelism: 1.000, latency: 180, iter: 1 },
  {workload: 'Transfer(0.0)', scheduling: 'advanced(16)', size: 2731, parallelism: 1.000, latency: 99, iter: 1 },
  {workload: 'Transfer(0.0)', scheduling: 'advanced(16)', size: 2049, parallelism: 1.000, latency: 72, iter: 1 },
  {workload: 'Transfer(0.0)', scheduling: 'advanced(24)', size: 65537, parallelism: 1.000, latency: 3743, iter: 1 },
  {workload: 'Transfer(0.0)', scheduling: 'advanced(24)', size: 32769, parallelism: 1.000, latency: 1658, iter: 1 },
  {workload: 'Transfer(0.0)', scheduling: 'advanced(24)', size: 16385, parallelism: 1.000, latency: 798, iter: 1 },
  {workload: 'Transfer(0.0)', scheduling: 'advanced(24)', size: 8193, parallelism: 1.000, latency: 391, iter: 1 },
  {workload: 'Transfer(0.0)', scheduling: 'advanced(24)', size: 4097, parallelism: 1.000, latency: 180, iter: 1 },
  {workload: 'Transfer(0.0)', scheduling: 'advanced(24)', size: 2731, parallelism: 1.000, latency: 100, iter: 1 },
  {workload: 'Transfer(0.0)', scheduling: 'advanced(24)', size: 2049, parallelism: 1.000, latency: 73, iter: 1 },
  {workload: 'Transfer(0.0)', scheduling: 'advanced(32)', size: 65537, parallelism: 1.000, latency: 3710, iter: 1 },
  {workload: 'Transfer(0.0)', scheduling: 'advanced(32)', size: 32769, parallelism: 1.000, latency: 1658, iter: 1 },
  {workload: 'Transfer(0.0)', scheduling: 'advanced(32)', size: 16385, parallelism: 1.000, latency: 800, iter: 1 },
  {workload: 'Transfer(0.0)', scheduling: 'advanced(32)', size: 8193, parallelism: 1.000, latency: 392, iter: 1 },
  {workload: 'Transfer(0.0)', scheduling: 'advanced(32)', size: 4097, parallelism: 1.000, latency: 180, iter: 1 },
  {workload: 'Transfer(0.0)', scheduling: 'advanced(32)', size: 2731, parallelism: 1.000, latency: 100, iter: 1 },
  {workload: 'Transfer(0.0)', scheduling: 'advanced(32)', size: 2049, parallelism: 1.000, latency: 72, iter: 1 },
  
  
  // Transfer(0.5): =======================================================================
  {workload: 'Transfer(0.5)', scheduling: 'basic', size: 65537, parallelism: 0.721, latency: 2010, iter: 1 },
  {workload: 'Transfer(0.5)', scheduling: 'basic', size: 32769, parallelism: 0.844, latency: 861, iter: 1 },
  {workload: 'Transfer(0.5)', scheduling: 'basic', size: 16385, parallelism: 0.918, latency: 397, iter: 1 },
  {workload: 'Transfer(0.5)', scheduling: 'basic', size: 8193, parallelism: 0.959, latency: 184, iter: 1 },
  {workload: 'Transfer(0.5)', scheduling: 'basic', size: 4097, parallelism: 0.980, latency: 82, iter: 1 },
  {workload: 'Transfer(0.5)', scheduling: 'basic', size: 2731, parallelism: 0.988, latency: 52, iter: 1 },
  {workload: 'Transfer(0.5)', scheduling: 'basic', size: 2049, parallelism: 0.992, latency: 38, iter: 1 },
  {workload: 'Transfer(0.5)', scheduling: 'basic', size: 65537, parallelism: 0.942, latency: 2556, iter: 2 },
  {workload: 'Transfer(0.5)', scheduling: 'basic', size: 32769, parallelism: 0.981, latency: 996, iter: 2 },
  {workload: 'Transfer(0.5)', scheduling: 'basic', size: 16385, parallelism: 0.995, latency: 433, iter: 2 },
  {workload: 'Transfer(0.5)', scheduling: 'basic', size: 8193, parallelism: 0.999, latency: 191, iter: 2 },
  {workload: 'Transfer(0.5)', scheduling: 'basic', size: 4097, parallelism: 1.000, latency: 84, iter: 2 },
  {workload: 'Transfer(0.5)', scheduling: 'basic', size: 2731, parallelism: 1.000, latency: 53, iter: 2 },
  {workload: 'Transfer(0.5)', scheduling: 'basic', size: 2049, parallelism: 1.000, latency: 39, iter: 2 },
  {workload: 'Transfer(0.5)', scheduling: 'basic', size: 65537, parallelism: 0.987, latency: 2666, iter: 3 },
  {workload: 'Transfer(0.5)', scheduling: 'basic', size: 32769, parallelism: 0.998, latency: 1016, iter: 3 },
  {workload: 'Transfer(0.5)', scheduling: 'basic', size: 16385, parallelism: 1.000, latency: 435, iter: 3 },
  {workload: 'Transfer(0.5)', scheduling: 'basic', size: 8193, parallelism: 1.000, latency: 191, iter: 3 },
  {workload: 'Transfer(0.5)', scheduling: 'basic', size: 4097, parallelism: 1.000, latency: 84, iter: 3 },
  {workload: 'Transfer(0.5)', scheduling: 'basic', size: 2731, parallelism: 1.000, latency: 53, iter: 3 },
  {workload: 'Transfer(0.5)', scheduling: 'basic', size: 2049, parallelism: 1.000, latency: 39, iter: 3 },
  
  {workload: 'Transfer(0.5)', scheduling: 'advanced(4)', size: 65537, parallelism: 0.721, latency: 3341, iter: 1 },
  {workload: 'Transfer(0.5)', scheduling: 'advanced(4)', size: 32769, parallelism: 0.844, latency: 1554, iter: 1 },
  {workload: 'Transfer(0.5)', scheduling: 'advanced(4)', size: 16385, parallelism: 0.918, latency: 770, iter: 1 },
  {workload: 'Transfer(0.5)', scheduling: 'advanced(4)', size: 8193, parallelism: 0.959, latency: 385, iter: 1 },
  {workload: 'Transfer(0.5)', scheduling: 'advanced(4)', size: 4097, parallelism: 0.980, latency: 179, iter: 1 },
  {workload: 'Transfer(0.5)', scheduling: 'advanced(4)', size: 2731, parallelism: 0.988, latency: 99, iter: 1 },
  {workload: 'Transfer(0.5)', scheduling: 'advanced(4)', size: 2049, parallelism: 0.992, latency: 72, iter: 1 },
  {workload: 'Transfer(0.5)', scheduling: 'advanced(4)', size: 65537, parallelism: 0.942, latency: 4267, iter: 2 },
  {workload: 'Transfer(0.5)', scheduling: 'advanced(4)', size: 32769, parallelism: 0.981, latency: 1810, iter: 2 },
  {workload: 'Transfer(0.5)', scheduling: 'advanced(4)', size: 16385, parallelism: 0.995, latency: 846, iter: 2 },
  {workload: 'Transfer(0.5)', scheduling: 'advanced(4)', size: 8193, parallelism: 0.999, latency: 400, iter: 2 },
  {workload: 'Transfer(0.5)', scheduling: 'advanced(4)', size: 4097, parallelism: 1.000, latency: 179, iter: 2 },
  {workload: 'Transfer(0.5)', scheduling: 'advanced(4)', size: 2731, parallelism: 1.000, latency: 101, iter: 2 },
  {workload: 'Transfer(0.5)', scheduling: 'advanced(4)', size: 2049, parallelism: 1.000, latency: 73, iter: 2 },
  {workload: 'Transfer(0.5)', scheduling: 'advanced(4)', size: 65537, parallelism: 0.987, latency: 4503, iter: 3 },
  {workload: 'Transfer(0.5)', scheduling: 'advanced(4)', size: 32769, parallelism: 0.998, latency: 1846, iter: 3 },
  {workload: 'Transfer(0.5)', scheduling: 'advanced(4)', size: 16385, parallelism: 1.000, latency: 849, iter: 3 },
  {workload: 'Transfer(0.5)', scheduling: 'advanced(4)', size: 8193, parallelism: 1.000, latency: 402, iter: 3 },
  {workload: 'Transfer(0.5)', scheduling: 'advanced(4)', size: 4097, parallelism: 1.000, latency: 183, iter: 3 },
  {workload: 'Transfer(0.5)', scheduling: 'advanced(4)', size: 2731, parallelism: 1.000, latency: 101, iter: 3 },
  {workload: 'Transfer(0.5)', scheduling: 'advanced(4)', size: 2049, parallelism: 1.000, latency: 73, iter: 3 },
  {workload: 'Transfer(0.5)', scheduling: 'advanced(8)', size: 65537, parallelism: 0.721, latency: 3379, iter: 1 },
  {workload: 'Transfer(0.5)', scheduling: 'advanced(8)', size: 32769, parallelism: 0.844, latency: 1560, iter: 1 },
  {workload: 'Transfer(0.5)', scheduling: 'advanced(8)', size: 16385, parallelism: 0.918, latency: 773, iter: 1 },
  {workload: 'Transfer(0.5)', scheduling: 'advanced(8)', size: 8193, parallelism: 0.959, latency: 385, iter: 1 },
  {workload: 'Transfer(0.5)', scheduling: 'advanced(8)', size: 4097, parallelism: 0.980, latency: 180, iter: 1 },
  {workload: 'Transfer(0.5)', scheduling: 'advanced(8)', size: 2731, parallelism: 0.988, latency: 99, iter: 1 },
  {workload: 'Transfer(0.5)', scheduling: 'advanced(8)', size: 2049, parallelism: 0.992, latency: 72, iter: 1 },
  {workload: 'Transfer(0.5)', scheduling: 'advanced(8)', size: 65537, parallelism: 0.942, latency: 4280, iter: 2 },
  {workload: 'Transfer(0.5)', scheduling: 'advanced(8)', size: 32769, parallelism: 0.981, latency: 1813, iter: 2 },
  {workload: 'Transfer(0.5)', scheduling: 'advanced(8)', size: 16385, parallelism: 0.995, latency: 846, iter: 2 },
  {workload: 'Transfer(0.5)', scheduling: 'advanced(8)', size: 8193, parallelism: 0.999, latency: 400, iter: 2 },
  {workload: 'Transfer(0.5)', scheduling: 'advanced(8)', size: 4097, parallelism: 1.000, latency: 180, iter: 2 },
  {workload: 'Transfer(0.5)', scheduling: 'advanced(8)', size: 2731, parallelism: 1.000, latency: 101, iter: 2 },
  {workload: 'Transfer(0.5)', scheduling: 'advanced(8)', size: 2049, parallelism: 1.000, latency: 73, iter: 2 },
  {workload: 'Transfer(0.5)', scheduling: 'advanced(8)', size: 65537, parallelism: 0.987, latency: 4484, iter: 3 },
  {workload: 'Transfer(0.5)', scheduling: 'advanced(8)', size: 32769, parallelism: 0.998, latency: 1845, iter: 3 },
  {workload: 'Transfer(0.5)', scheduling: 'advanced(8)', size: 16385, parallelism: 1.000, latency: 850, iter: 3 },
  {workload: 'Transfer(0.5)', scheduling: 'advanced(8)', size: 8193, parallelism: 1.000, latency: 403, iter: 3 },
  {workload: 'Transfer(0.5)', scheduling: 'advanced(8)', size: 4097, parallelism: 1.000, latency: 183, iter: 3 },
  {workload: 'Transfer(0.5)', scheduling: 'advanced(8)', size: 2731, parallelism: 1.000, latency: 100, iter: 3 },
  {workload: 'Transfer(0.5)', scheduling: 'advanced(8)', size: 2049, parallelism: 1.000, latency: 73, iter: 3 },
  {workload: 'Transfer(0.5)', scheduling: 'advanced(16)', size: 65537, parallelism: 0.721, latency: 3348, iter: 1 },
  {workload: 'Transfer(0.5)', scheduling: 'advanced(16)', size: 32769, parallelism: 0.844, latency: 1558, iter: 1 },
  {workload: 'Transfer(0.5)', scheduling: 'advanced(16)', size: 16385, parallelism: 0.918, latency: 771, iter: 1 },
  {workload: 'Transfer(0.5)', scheduling: 'advanced(16)', size: 8193, parallelism: 0.959, latency: 385, iter: 1 },
  {workload: 'Transfer(0.5)', scheduling: 'advanced(16)', size: 4097, parallelism: 0.980, latency: 179, iter: 1 },
  {workload: 'Transfer(0.5)', scheduling: 'advanced(16)', size: 2731, parallelism: 0.988, latency: 101, iter: 1 },
  {workload: 'Transfer(0.5)', scheduling: 'advanced(16)', size: 2049, parallelism: 0.992, latency: 72, iter: 1 },
  {workload: 'Transfer(0.5)', scheduling: 'advanced(16)', size: 65537, parallelism: 0.942, latency: 4321, iter: 2 },
  {workload: 'Transfer(0.5)', scheduling: 'advanced(16)', size: 32769, parallelism: 0.981, latency: 1820, iter: 2 },
  {workload: 'Transfer(0.5)', scheduling: 'advanced(16)', size: 16385, parallelism: 0.995, latency: 848, iter: 2 },
  {workload: 'Transfer(0.5)', scheduling: 'advanced(16)', size: 8193, parallelism: 0.999, latency: 403, iter: 2 },
  {workload: 'Transfer(0.5)', scheduling: 'advanced(16)', size: 4097, parallelism: 1.000, latency: 180, iter: 2 },
  {workload: 'Transfer(0.5)', scheduling: 'advanced(16)', size: 2731, parallelism: 1.000, latency: 102, iter: 2 },
  {workload: 'Transfer(0.5)', scheduling: 'advanced(16)', size: 2049, parallelism: 1.000, latency: 74, iter: 2 },
  {workload: 'Transfer(0.5)', scheduling: 'advanced(16)', size: 65537, parallelism: 0.987, latency: 4536, iter: 3 },
  {workload: 'Transfer(0.5)', scheduling: 'advanced(16)', size: 32769, parallelism: 0.998, latency: 1850, iter: 3 },
  {workload: 'Transfer(0.5)', scheduling: 'advanced(16)', size: 16385, parallelism: 1.000, latency: 850, iter: 3 },
  {workload: 'Transfer(0.5)', scheduling: 'advanced(16)', size: 8193, parallelism: 1.000, latency: 403, iter: 3 },
  {workload: 'Transfer(0.5)', scheduling: 'advanced(16)', size: 4097, parallelism: 1.000, latency: 183, iter: 3 },
  {workload: 'Transfer(0.5)', scheduling: 'advanced(16)', size: 2731, parallelism: 1.000, latency: 102, iter: 3 },
  {workload: 'Transfer(0.5)', scheduling: 'advanced(16)', size: 2049, parallelism: 1.000, latency: 74, iter: 3 },
  {workload: 'Transfer(0.5)', scheduling: 'advanced(24)', size: 65537, parallelism: 0.721, latency: 3350, iter: 1 },
  {workload: 'Transfer(0.5)', scheduling: 'advanced(24)', size: 32769, parallelism: 0.844, latency: 1558, iter: 1 },
  {workload: 'Transfer(0.5)', scheduling: 'advanced(24)', size: 16385, parallelism: 0.918, latency: 773, iter: 1 },
  {workload: 'Transfer(0.5)', scheduling: 'advanced(24)', size: 8193, parallelism: 0.959, latency: 386, iter: 1 },
  {workload: 'Transfer(0.5)', scheduling: 'advanced(24)', size: 4097, parallelism: 0.980, latency: 179, iter: 1 },
  {workload: 'Transfer(0.5)', scheduling: 'advanced(24)', size: 2731, parallelism: 0.988, latency: 100, iter: 1 },
  {workload: 'Transfer(0.5)', scheduling: 'advanced(24)', size: 2049, parallelism: 0.992, latency: 72, iter: 1 },
  {workload: 'Transfer(0.5)', scheduling: 'advanced(24)', size: 65537, parallelism: 0.942, latency: 4265, iter: 2 },
  {workload: 'Transfer(0.5)', scheduling: 'advanced(24)', size: 32769, parallelism: 0.981, latency: 1814, iter: 2 },
  {workload: 'Transfer(0.5)', scheduling: 'advanced(24)', size: 16385, parallelism: 0.995, latency: 846, iter: 2 },
  {workload: 'Transfer(0.5)', scheduling: 'advanced(24)', size: 8193, parallelism: 0.999, latency: 400, iter: 2 },
  {workload: 'Transfer(0.5)', scheduling: 'advanced(24)', size: 4097, parallelism: 1.000, latency: 179, iter: 2 },
  {workload: 'Transfer(0.5)', scheduling: 'advanced(24)', size: 2731, parallelism: 1.000, latency: 101, iter: 2 },
  {workload: 'Transfer(0.5)', scheduling: 'advanced(24)', size: 2049, parallelism: 1.000, latency: 73, iter: 2 },
  {workload: 'Transfer(0.5)', scheduling: 'advanced(24)', size: 65537, parallelism: 0.987, latency: 4484, iter: 3 },
  {workload: 'Transfer(0.5)', scheduling: 'advanced(24)', size: 32769, parallelism: 0.998, latency: 1848, iter: 3 },
  {workload: 'Transfer(0.5)', scheduling: 'advanced(24)', size: 16385, parallelism: 1.000, latency: 847, iter: 3 },
  {workload: 'Transfer(0.5)', scheduling: 'advanced(24)', size: 8193, parallelism: 1.000, latency: 403, iter: 3 },
  {workload: 'Transfer(0.5)', scheduling: 'advanced(24)', size: 4097, parallelism: 1.000, latency: 183, iter: 3 },
  {workload: 'Transfer(0.5)', scheduling: 'advanced(24)', size: 2731, parallelism: 1.000, latency: 102, iter: 3 },
  {workload: 'Transfer(0.5)', scheduling: 'advanced(24)', size: 2049, parallelism: 1.000, latency: 73, iter: 3 },
  {workload: 'Transfer(0.5)', scheduling: 'advanced(32)', size: 65537, parallelism: 0.721, latency: 3354, iter: 1 },
  {workload: 'Transfer(0.5)', scheduling: 'advanced(32)', size: 32769, parallelism: 0.844, latency: 1563, iter: 1 },
  {workload: 'Transfer(0.5)', scheduling: 'advanced(32)', size: 16385, parallelism: 0.918, latency: 775, iter: 1 },
  {workload: 'Transfer(0.5)', scheduling: 'advanced(32)', size: 8193, parallelism: 0.959, latency: 386, iter: 1 },
  {workload: 'Transfer(0.5)', scheduling: 'advanced(32)', size: 4097, parallelism: 0.980, latency: 179, iter: 1 },
  {workload: 'Transfer(0.5)', scheduling: 'advanced(32)', size: 2731, parallelism: 0.988, latency: 101, iter: 1 },
  {workload: 'Transfer(0.5)', scheduling: 'advanced(32)', size: 2049, parallelism: 0.992, latency: 72, iter: 1 },
  {workload: 'Transfer(0.5)', scheduling: 'advanced(32)', size: 65537, parallelism: 0.942, latency: 4290, iter: 2 },
  {workload: 'Transfer(0.5)', scheduling: 'advanced(32)', size: 32769, parallelism: 0.981, latency: 1818, iter: 2 },
  {workload: 'Transfer(0.5)', scheduling: 'advanced(32)', size: 16385, parallelism: 0.995, latency: 846, iter: 2 },
  {workload: 'Transfer(0.5)', scheduling: 'advanced(32)', size: 8193, parallelism: 0.999, latency: 401, iter: 2 },
  {workload: 'Transfer(0.5)', scheduling: 'advanced(32)', size: 4097, parallelism: 1.000, latency: 179, iter: 2 },
  {workload: 'Transfer(0.5)', scheduling: 'advanced(32)', size: 2731, parallelism: 1.000, latency: 102, iter: 2 },
  {workload: 'Transfer(0.5)', scheduling: 'advanced(32)', size: 2049, parallelism: 1.000, latency: 74, iter: 2 },
  {workload: 'Transfer(0.5)', scheduling: 'advanced(32)', size: 65537, parallelism: 0.987, latency: 4489, iter: 3 },
  {workload: 'Transfer(0.5)', scheduling: 'advanced(32)', size: 32769, parallelism: 0.998, latency: 1853, iter: 3 },
  {workload: 'Transfer(0.5)', scheduling: 'advanced(32)', size: 16385, parallelism: 1.000, latency: 850, iter: 3 },
  {workload: 'Transfer(0.5)', scheduling: 'advanced(32)', size: 8193, parallelism: 1.000, latency: 403, iter: 3 },
  {workload: 'Transfer(0.5)', scheduling: 'advanced(32)', size: 4097, parallelism: 1.000, latency: 184, iter: 3 },
  {workload: 'Transfer(0.5)', scheduling: 'advanced(32)', size: 2731, parallelism: 1.000, latency: 103, iter: 3 },
  {workload: 'Transfer(0.5)', scheduling: 'advanced(32)', size: 2049, parallelism: 1.000, latency: 74, iter: 3 },
  
  // DHashMap: ============================================================================
  {workload: 'DHashMap', scheduling: 'basic', size: 65537, parallelism: 0.513, latency: 1510, iter: 1 },
  {workload: 'DHashMap', scheduling: 'basic', size: 32769, parallelism: 0.532, latency: 760, iter: 1 },
  {workload: 'DHashMap', scheduling: 'basic', size: 16385, parallelism: 0.565, latency: 388, iter: 1 },
  {workload: 'DHashMap', scheduling: 'basic', size: 8193, parallelism: 0.625, latency: 198, iter: 1 },
  {workload: 'DHashMap', scheduling: 'basic', size: 4097, parallelism: 0.707, latency: 91, iter: 1 },
  {workload: 'DHashMap', scheduling: 'basic', size: 2731, parallelism: 0.760, latency: 57, iter: 1 },
  {workload: 'DHashMap', scheduling: 'basic', size: 2049, parallelism: 0.803, latency: 41, iter: 1 },
  {workload: 'DHashMap', scheduling: 'basic', size: 65537, parallelism: 0.529, latency: 2743, iter: 2 },
  {workload: 'DHashMap', scheduling: 'basic', size: 32769, parallelism: 0.563, latency: 1362, iter: 2 },
  {workload: 'DHashMap', scheduling: 'basic', size: 16385, parallelism: 0.628, latency: 680, iter: 2 },
  {workload: 'DHashMap', scheduling: 'basic', size: 8193, parallelism: 0.739, latency: 317, iter: 2 },
  {workload: 'DHashMap', scheduling: 'basic', size: 4097, parallelism: 0.860, latency: 134, iter: 2 },
  {workload: 'DHashMap', scheduling: 'basic', size: 2731, parallelism: 0.911, latency: 80, iter: 2 },
  {workload: 'DHashMap', scheduling: 'basic', size: 2049, parallelism: 0.940, latency: 56, iter: 2 },
  {workload: 'DHashMap', scheduling: 'basic', size: 65537, parallelism: 0.545, latency: 3946, iter: 3 },
  {workload: 'DHashMap', scheduling: 'basic', size: 32769, parallelism: 0.594, latency: 1934, iter: 3 },
  {workload: 'DHashMap', scheduling: 'basic', size: 16385, parallelism: 0.689, latency: 927, iter: 3 },
  {workload: 'DHashMap', scheduling: 'basic', size: 8193, parallelism: 0.832, latency: 399, iter: 3 },
  {workload: 'DHashMap', scheduling: 'basic', size: 4097, parallelism: 0.945, latency: 155, iter: 3 },
  {workload: 'DHashMap', scheduling: 'basic', size: 2731, parallelism: 0.973, latency: 90, iter: 3 },
  {workload: 'DHashMap', scheduling: 'basic', size: 2049, parallelism: 0.984, latency: 61, iter: 3 },
  {workload: 'DHashMap', scheduling: 'basic', size: 65537, parallelism: 0.560, latency: 5095, iter: 4 },
  {workload: 'DHashMap', scheduling: 'basic', size: 32769, parallelism: 0.625, latency: 2468, iter: 4 },
  {workload: 'DHashMap', scheduling: 'basic', size: 16385, parallelism: 0.749, latency: 1133, iter: 4 },
  {workload: 'DHashMap', scheduling: 'basic', size: 8193, parallelism: 0.903, latency: 455, iter: 4 },
  {workload: 'DHashMap', scheduling: 'basic', size: 4097, parallelism: 0.980, latency: 165, iter: 4 },
  {workload: 'DHashMap', scheduling: 'basic', size: 2731, parallelism: 0.993, latency: 92, iter: 4 },
  {workload: 'DHashMap', scheduling: 'basic', size: 2049, parallelism: 0.997, latency: 62, iter: 4 },
  {workload: 'DHashMap', scheduling: 'basic', size: 65537, parallelism: 0.576, latency: 6218, iter: 5 },
  {workload: 'DHashMap', scheduling: 'basic', size: 32769, parallelism: 0.657, latency: 2959, iter: 5 },
  {workload: 'DHashMap', scheduling: 'basic', size: 16385, parallelism: 0.805, latency: 1300, iter: 5 },
  {workload: 'DHashMap', scheduling: 'basic', size: 8193, parallelism: 0.949, latency: 489, iter: 5 },
  {workload: 'DHashMap', scheduling: 'basic', size: 4097, parallelism: 0.994, latency: 170, iter: 5 },
  {workload: 'DHashMap', scheduling: 'basic', size: 2731, parallelism: 0.999, latency: 94, iter: 5 },
  {workload: 'DHashMap', scheduling: 'basic', size: 2049, parallelism: 1.000, latency: 63, iter: 5 },
  
  {workload: 'DHashMap', scheduling: 'advanced(4)', size: 65537, parallelism: 1.000, latency: 1775, iter: 1 },
  {workload: 'DHashMap', scheduling: 'advanced(4)', size: 32769, parallelism: 1.000, latency: 901, iter: 1 },
  {workload: 'DHashMap', scheduling: 'advanced(4)', size: 16385, parallelism: 1.000, latency: 436, iter: 1 },
  {workload: 'DHashMap', scheduling: 'advanced(4)', size: 8193, parallelism: 1.000, latency: 231, iter: 1 },
  {workload: 'DHashMap', scheduling: 'advanced(4)', size: 4097, parallelism: 1.000, latency: 123, iter: 1 },
  {workload: 'DHashMap', scheduling: 'advanced(4)', size: 2731, parallelism: 1.000, latency: 81, iter: 1 },
  {workload: 'DHashMap', scheduling: 'advanced(4)', size: 2049, parallelism: 1.000, latency: 59, iter: 1 },
  {workload: 'DHashMap', scheduling: 'advanced(8)', size: 65537, parallelism: 1.000, latency: 1790, iter: 1 },
  {workload: 'DHashMap', scheduling: 'advanced(8)', size: 32769, parallelism: 1.000, latency: 908, iter: 1 },
  {workload: 'DHashMap', scheduling: 'advanced(8)', size: 16385, parallelism: 1.000, latency: 439, iter: 1 },
  {workload: 'DHashMap', scheduling: 'advanced(8)', size: 8193, parallelism: 1.000, latency: 233, iter: 1 },
  {workload: 'DHashMap', scheduling: 'advanced(8)', size: 4097, parallelism: 1.000, latency: 122, iter: 1 },
  {workload: 'DHashMap', scheduling: 'advanced(8)', size: 2731, parallelism: 1.000, latency: 81, iter: 1 },
  {workload: 'DHashMap', scheduling: 'advanced(8)', size: 2049, parallelism: 1.000, latency: 59, iter: 1 },
  {workload: 'DHashMap', scheduling: 'advanced(16)', size: 65537, parallelism: 1.000, latency: 1807, iter: 1 },
  {workload: 'DHashMap', scheduling: 'advanced(16)', size: 32769, parallelism: 1.000, latency: 922, iter: 1 },
  {workload: 'DHashMap', scheduling: 'advanced(16)', size: 16385, parallelism: 1.000, latency: 444, iter: 1 },
  {workload: 'DHashMap', scheduling: 'advanced(16)', size: 8193, parallelism: 1.000, latency: 235, iter: 1 },
  {workload: 'DHashMap', scheduling: 'advanced(16)', size: 4097, parallelism: 1.000, latency: 125, iter: 1 },
  {workload: 'DHashMap', scheduling: 'advanced(16)', size: 2731, parallelism: 1.000, latency: 82, iter: 1 },
  {workload: 'DHashMap', scheduling: 'advanced(16)', size: 2049, parallelism: 1.000, latency: 60, iter: 1 },
  {workload: 'DHashMap', scheduling: 'advanced(24)', size: 65537, parallelism: 1.000, latency: 1828, iter: 1 },
  {workload: 'DHashMap', scheduling: 'advanced(24)', size: 32769, parallelism: 1.000, latency: 928, iter: 1 },
  {workload: 'DHashMap', scheduling: 'advanced(24)', size: 16385, parallelism: 1.000, latency: 448, iter: 1 },
  {workload: 'DHashMap', scheduling: 'advanced(24)', size: 8193, parallelism: 1.000, latency: 236, iter: 1 },
  {workload: 'DHashMap', scheduling: 'advanced(24)', size: 4097, parallelism: 1.000, latency: 124, iter: 1 },
  {workload: 'DHashMap', scheduling: 'advanced(24)', size: 2731, parallelism: 1.000, latency: 82, iter: 1 },
  {workload: 'DHashMap', scheduling: 'advanced(24)', size: 2049, parallelism: 1.000, latency: 59, iter: 1 },
  {workload: 'DHashMap', scheduling: 'advanced(32)', size: 65537, parallelism: 1.000, latency: 1843, iter: 1 },
  {workload: 'DHashMap', scheduling: 'advanced(32)', size: 32769, parallelism: 1.000, latency: 940, iter: 1 },
  {workload: 'DHashMap', scheduling: 'advanced(32)', size: 16385, parallelism: 1.000, latency: 451, iter: 1 },
  {workload: 'DHashMap', scheduling: 'advanced(32)', size: 8193, parallelism: 1.000, latency: 238, iter: 1 },
  {workload: 'DHashMap', scheduling: 'advanced(32)', size: 4097, parallelism: 1.000, latency: 123, iter: 1 },
  {workload: 'DHashMap', scheduling: 'advanced(32)', size: 2731, parallelism: 1.000, latency: 81, iter: 1 },
  {workload: 'DHashMap', scheduling: 'advanced(32)', size: 2049, parallelism: 1.000, latency: 59, iter: 1 },
  
  // Fibonacci: ============================================================================
  {workload: 'Fibonacci', scheduling: 'basic', size: 65537, parallelism: 1.000, latency: 155, iter: 1 },
  {workload: 'Fibonacci', scheduling: 'basic', size: 32769, parallelism: 1.000, latency: 77, iter: 1 },
  {workload: 'Fibonacci', scheduling: 'basic', size: 16385, parallelism: 1.000, latency: 39, iter: 1 },
  {workload: 'Fibonacci', scheduling: 'basic', size: 8193, parallelism: 1.000, latency: 19, iter: 1 },
  {workload: 'Fibonacci', scheduling: 'basic', size: 4097, parallelism: 1.000, latency: 9, iter: 1 },
  {workload: 'Fibonacci', scheduling: 'basic', size: 2731, parallelism: 1.000, latency: 6, iter: 1 },
  {workload: 'Fibonacci', scheduling: 'basic', size: 2049, parallelism: 1.000, latency: 4, iter: 1 },
  
  {workload: 'Fibonacci', scheduling: 'advanced(4)', size: 65537, parallelism: 1.000, latency: 127, iter: 1 },
  {workload: 'Fibonacci', scheduling: 'advanced(4)', size: 32769, parallelism: 1.000, latency: 63, iter: 1 },
  {workload: 'Fibonacci', scheduling: 'advanced(4)', size: 16385, parallelism: 1.000, latency: 31, iter: 1 },
  {workload: 'Fibonacci', scheduling: 'advanced(4)', size: 8193, parallelism: 1.000, latency: 15, iter: 1 },
  {workload: 'Fibonacci', scheduling: 'advanced(4)', size: 4097, parallelism: 1.000, latency: 7, iter: 1 },
  {workload: 'Fibonacci', scheduling: 'advanced(4)', size: 2731, parallelism: 1.000, latency: 5, iter: 1 },
  {workload: 'Fibonacci', scheduling: 'advanced(4)', size: 2049, parallelism: 1.000, latency: 3, iter: 1 },
  {workload: 'Fibonacci', scheduling: 'advanced(8)', size: 65537, parallelism: 1.000, latency: 127, iter: 1 },
  {workload: 'Fibonacci', scheduling: 'advanced(8)', size: 32769, parallelism: 1.000, latency: 62, iter: 1 },
  {workload: 'Fibonacci', scheduling: 'advanced(8)', size: 16385, parallelism: 1.000, latency: 31, iter: 1 },
  {workload: 'Fibonacci', scheduling: 'advanced(8)', size: 8193, parallelism: 1.000, latency: 15, iter: 1 },
  {workload: 'Fibonacci', scheduling: 'advanced(8)', size: 4097, parallelism: 1.000, latency: 7, iter: 1 },
  {workload: 'Fibonacci', scheduling: 'advanced(8)', size: 2731, parallelism: 1.000, latency: 5, iter: 1 },
  {workload: 'Fibonacci', scheduling: 'advanced(8)', size: 2049, parallelism: 1.000, latency: 3, iter: 1 },
  {workload: 'Fibonacci', scheduling: 'advanced(16)', size: 65537, parallelism: 1.000, latency: 127, iter: 1 },
  {workload: 'Fibonacci', scheduling: 'advanced(16)', size: 32769, parallelism: 1.000, latency: 62, iter: 1 },
  {workload: 'Fibonacci', scheduling: 'advanced(16)', size: 16385, parallelism: 1.000, latency: 31, iter: 1 },
  {workload: 'Fibonacci', scheduling: 'advanced(16)', size: 8193, parallelism: 1.000, latency: 15, iter: 1 },
  {workload: 'Fibonacci', scheduling: 'advanced(16)', size: 4097, parallelism: 1.000, latency: 7, iter: 1 },
  {workload: 'Fibonacci', scheduling: 'advanced(16)', size: 2731, parallelism: 1.000, latency: 5, iter: 1 },
  {workload: 'Fibonacci', scheduling: 'advanced(16)', size: 2049, parallelism: 1.000, latency: 3, iter: 1 },
  {workload: 'Fibonacci', scheduling: 'advanced(24)', size: 65537, parallelism: 1.000, latency: 128, iter: 1 },
  {workload: 'Fibonacci', scheduling: 'advanced(24)', size: 32769, parallelism: 1.000, latency: 63, iter: 1 },
  {workload: 'Fibonacci', scheduling: 'advanced(24)', size: 16385, parallelism: 1.000, latency: 31, iter: 1 },
  {workload: 'Fibonacci', scheduling: 'advanced(24)', size: 8193, parallelism: 1.000, latency: 15, iter: 1 },
  {workload: 'Fibonacci', scheduling: 'advanced(24)', size: 4097, parallelism: 1.000, latency: 7, iter: 1 },
  {workload: 'Fibonacci', scheduling: 'advanced(24)', size: 2731, parallelism: 1.000, latency: 5, iter: 1 },
  {workload: 'Fibonacci', scheduling: 'advanced(24)', size: 2049, parallelism: 1.000, latency: 3, iter: 1 },
  {workload: 'Fibonacci', scheduling: 'advanced(32)', size: 65537, parallelism: 1.000, latency: 127, iter: 1 },
  {workload: 'Fibonacci', scheduling: 'advanced(32)', size: 32769, parallelism: 1.000, latency: 63, iter: 1 },
  {workload: 'Fibonacci', scheduling: 'advanced(32)', size: 16385, parallelism: 1.000, latency: 31, iter: 1 },
  {workload: 'Fibonacci', scheduling: 'advanced(32)', size: 8193, parallelism: 1.000, latency: 15, iter: 1 },
  {workload: 'Fibonacci', scheduling: 'advanced(32)', size: 4097, parallelism: 1.000, latency: 7, iter: 1 },
  {workload: 'Fibonacci', scheduling: 'advanced(32)', size: 2731, parallelism: 1.000, latency: 5, iter: 1 },
  {workload: 'Fibonacci', scheduling: 'advanced(32)', size: 2049, parallelism: 1.000, latency: 3, iter: 1 },
    ]
  };
  
  let make_dataset = (scheduling, iter, size, workload, id) => {
    let transforms = [{
      type: 'filter',
      config: { dimension: 'scheduling', value: scheduling },
    }];
    if (iter) {
      transforms.push({
          type: 'filter',
          config: { dimension: 'iter', value: iter }
        })
    }
  
    if (size) {
      transforms.push({
          type: 'filter',
          config: { dimension: 'size', value: size }
        })
    }
  
    transforms.push({
          type: 'filter',
          config: { dimension: 'workload', value: workload }
        })
  
    return {
        id: id,
        transform: transforms
      };
  }
  
  let make_serie = (name, config) => {
    return {
        name: name,
        symbolSize: 30,
        datasetId: name,
        type: config.serie_type,
        encode: { x: config.encode_x, y: config.encode_y},
        label: {
          fontSize: 20,
          show: config.show_point_label,
          formatter: function (param) {
            return param.data[config.point_label];
          },
        }
      };
  };
  
  // // Latency vs parallelism (scatter chunk size)
  let config_latency_parallelism_size = {
      encode_y: 'latency',
      iter: 1,
      size: null,
      serie_type: 'line',
      point_label: 'size',
      show_point_label: true,
      encode_x: 'parallelism',
      xAxis: {
        name: 'Completion',
        nameGap: 40,
        nameLocation: 'middle',
        nameTextStyle: { fontSize: 24, },
        axisLabel: {
          fontSize: 20,
          formatter: (value) => value * 100 + '%',
        },
      }
  };
  
  // // Latency vs parallelism (scatter iter)
  let config_latency_parallelism_iter = {
      encode_y: 'latency',
      iter: null,
      size: 65537,
      serie_type: 'scatter',
      point_label: 'iter',
      show_point_label: true,
      encode_x: 'parallelism',
      xAxis: {
        name: 'Completion',
        nameGap: 40,
        nameLocation: 'middle',
        nameTextStyle: { fontSize: 24, },
        axisLabel: {
          fontSize: 20,
          formatter: (value) => value * 100 + '%',
        },
      }
  }
  
  // // Latency vs chunk size
  let config_latency_size = {
      encode_y: 'latency',
      iter: 1,
      size: null,
      show_point_label: false,
      serie_type: 'line',
      encode_x: 'size',
      xAxis: {
          // inverse: true,
          name: 'Batch size',
          nameGap: 40,
          nameLocation: 'middle',
          nameTextStyle: { fontSize: 24, },
          axisLabel: {
            fontSize: 20
          },
      }
  }
  
  // =====================================================================
  // let config = config_latency_size;
  let config = config_latency_parallelism_iter;
  // let config = config_latency_parallelism_size;
  
  config.datasets = [
    /*
    * The first argument filters the scheduling type:
    *    basic        => basic scheduling
    *    advanced(4)  => avanced scheduling assuming 4 available execution cores
    *
    * The last argument is the id of the dataset, which is referenced by the series
    */
    // make_dataset('basic', config.iter, config.size, "Transfer(0.0)", 'Transfer 0%'),
    // make_dataset('basic', config.iter, config.size, 'Transfer(0.5)', 'Transfer 50%'),
    // make_dataset('basic', config.iter, config.size, "DHashMap", 'Hashmap 50%'),
    // make_dataset('basic', config.iter, config.size, "Fibonacci", 'Fibonacci 20'),
    
    make_dataset('advanced(8)', config.iter, config.size, "Transfer(0.0)", 'Transfer 0%'),
    make_dataset('advanced(8)', config.iter, config.size, 'Transfer(0.5)', 'Transfer 50%'),
    make_dataset('advanced(8)', config.iter, config.size, "DHashMap", 'Hashmap 50%'),
    make_dataset('advanced(8)', config.iter, config.size, "Fibonacci", 'Fibonacci 20'),
    
    // make_dataset('advanced(16)', config.iter, config.size, "Transfer(0.0)", 'Transfer 0%'),
    // make_dataset('advanced(16)', config.iter, config.size, 'Transfer(0.5)', 'Transfer 50%'),
    // make_dataset('advanced(16)', config.iter, config.size, "DHashMap", 'Hashmap 50%'),
    // make_dataset('advanced(16)', config.iter, config.size, "Fibonacci", 'Fibonacci 20'),
  ];
  
  let series = [
    make_serie('Transfer 0%', config),
    make_serie('Transfer 50%', config),
    make_serie('Hashmap 50%', config),
    make_serie('Fibonacci 20', config),
  ];
  
  option = {
    title: {
      left: '40%',
      text: 'Advanced scheduling',
      textStyle: { fontSize: 24, },
    },
    toolbox: {
      feature: {
        saveAsImage: {}
      }
    },
    dataset: [
      data,
      ...config.datasets
    ],
    // dataZoom: [
    //   { yAxisIndex: 0, },
    //   { xAxisIndex: 0, }
    // ],
    legend: {
      right: '0%',
      textStyle: { fontSize: 22, },
      orient: 'vertical',
    },
    tooltip: {
      trigger: 'axis',
      valueFormatter: (value) => value + ' µs',
    },
    grid: {
      right: '5%'
    },
    xAxis: config.xAxis,
    yAxis: {
      max: 7000,
      name: 'Latency',
      type: 'value',
      nameTextStyle: { fontSize: 24, },
      axisLabel: {
        formatter: '{value} µs',
        fontSize: 22
      },
    },
    series: series
  };
