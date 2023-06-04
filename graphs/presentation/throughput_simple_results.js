let chart_type = 'line';
// -----------------------------------------------------------------------------
let title = 'Hashmap (10% insertions)'// 1024 buckets
let series = [
  { type: chart_type, data: [
    // 2 schedulers
    // ['sequential', 6.08], // 6.08 ± 0.00 tx/µs
    ['2 cores', 9.19],     // 9.19 ± 0.01 tx/µs
    ['4 cores', 14.27],     // 14.27 ± 0.01 tx/µs
    ['8 cores', 18.51],     // 18.51 ± 0.01 tx/µs
    ['12 cores', 18.13],    // 18.13 ± 0.02 tx/µs
    ['16 cores', 17.27],    // 17.27 ± 0.01 tx/µs
    ['22 cores', 15.35],    // 15.35 ± 0.01 tx/µs
  ]},
  {
    name: 'Sequential', data: [], type: 'scatter',
    markLine: {
      data: [{ yAxis: 6.08 }],
      // label: { formatter: 'Sequential'},
    },
  },
];
// // -----------------------------------------------------------------------------
// let title = 'Hashmap (50% insertions)'// 1024 buckets
// let series = [
//   { type: 'bar', data: [
//     // 4 schedulers
//     // ['sequential', 5.36], // 5.36 ± 0.00 tx/µs
//     ['2 cores', 6.82],     // 6.82 ± 0.01 tx/µs
//     ['4 cores', 9.54],     // 9.54 ± 0.01 tx/µs
//     ['8 cores', 11.53],     // 11.53 ± 0.01 tx/µs
//     ['12 cores', 10.51],    // 10.51 ± 0.01 tx/µs
//     ['16 cores', 8.76],    // 8.76 ± 0.02 tx/µs
//     ['22 cores', 7.42],    // 7.42 ± 0.01 tx/µs
//   ]},
//   {
//     name: 'Sequential', data: [], type: scatter,
//     markLine: {
//       data: [{ yAxis: 5.36 }],
//       // label: { formatter: 'Sequential'},
//     },
//   },
// ];
// // -----------------------------------------------------------------------------
// let title = 'Transfers (no conflicts)'
// let series = [
//   { type: 'bar', data: [
//     // 8 schedulers
//     // ['sequential', 34.84], // 34.84 ± 0.30 tx/µs
//     ['2 cores', 51.44],     // 51.44 ± 0.24 tx/µs
//     ['4 cores', 72.90],     // 72.90 ± 0.08 tx/µs
//     ['8 cores', 85.56],     // 16 cores 85.56 ± 0.11 tx/µs
//     ['12 cores', 83.91],    // 83.91 ± 0.11 tx/µs
//     ['16 cores', 75.07],    // 75.07 ± 0.09 tx/µs
//     ['22 cores', 56.69],    // 56.69 ± 0.05 tx/µs
//   ]},
//   {
//     name: 'Sequential', data: [], type: scatter,
//     markLine: {
//       data: [{ yAxis: 34.84 }],
//       // label: { formatter: 'Sequential'},
//     },
//   },
// ];
// // -----------------------------------------------------------------------------
// let title = 'Transfers (50% conflicts)'
// let series = [
//   { type: 'bar', data: [
//     // 4 schedulers
//     // ['sequential', 35.25], // 35.25 ± 0.34 tx/µs
//     ['2 cores', 43.52],     // 43.52 ± 0.12 tx/µs
//     ['4 cores', 50.22],     // 50.22 ± 0.08 tx/µs
//     ['8 cores', 51.64],     // 51.64 ± 0.04 tx/µs
//     ['12 cores', 48.83],    // 48.83 ± 0.04 tx/µs
//     ['16 cores', 39.62],    // 39.62 ± 0.02 tx/µs
//     ['22 cores', 34.44],    // 34.44 ± 0.05 tx/µs
//   ]},
//   {
//     name: 'Sequential', data: [], type: scatter,
//     markLine: {
//       data: [{ yAxis: 35.25 }],
//       // label: { formatter: 'Sequential'},
//     },
//   },
// ];
// // -----------------------------------------------------------------------------
// let title = 'Fib(20)'
// let series = [
//   { type: 'bar', data: [
//     // ['sequential', 0.05], // 0.05 ± 0.00 tx/µs
//     ['2 cores', 0.09],     // 0.09 ± 0.00 tx/µs
//     ['4 cores', 0.19],     // 0.19 ± 0.00 tx/µs
//     ['8 cores', 0.37],     // 0.37 ± 0.00 tx/µs
//     ['12 cores', 0.32],    // 0.32 ± 0.00 tx/µs  // outlier?
//     // ['12 cores', 0.43],    // 0.43 ± 0.01 tx/µs c.f. more_benchmarks.out
//     // ['16 cores', 0.44],    // 0.44 ± 0.00 tx/µs c.f. more_benchmarks.out
//     // ['22 cores', 56.69],    // 56.69 ± 0.05 tx/µs
//   ]},
//   {
//     name: 'Sequential', data: [], type: scatter,
//     markLine: {
//       data: [{ yAxis: 0.05 }],
//       // label: { formatter: 'Sequential'},
//     },
//   },
// ];

// let executors =  ['sequential', 2, 4, 8, 12, 16, 22];
option = {
  title: {
    text: title,
    left: 'center',
  },
  tooltip: {
    trigger: 'axis',
    valueFormatter: (value) => value + ' Millions tx/s'
  },
  xAxis: {
    type: 'category',
    // data: executors
  },
  yAxis: {
    type: 'value',
    name: 'Throughput\n(Million tx/s)',
    // axisLabel: { formatter: '{value} Million tx/s'},
    axisLine: {
      show: true
    },
  },
  series: [
    ...series
  ]
};