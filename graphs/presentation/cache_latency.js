// sudo perf stat -e L1-icache-load-misses,L1-dcache-load-misses,l2_rqsts.all_demand_miss,LLC-load-misses,LLC-store-misses,dTLB-load-misses,dTLB-store-misses ./target/release/testbench
// 3 schedulers, 4 executors
option = {
  title: {
    text: "Cache misses"
  },
  toolbox: {
    feature: {
      saveAsImage: {}
    }
  },
  legend: {},
  tooltip: {
    trigger: 'axis',
  },
  dataZoom: [
    { xAxisIndex: 0},
    { yAxisIndex: 0 }
  ],
  xAxis: {
    type: 'category',
    data: [
      'L1i-load',
      'L1d-load',
      'L2', // l2_rqsts.all_demand_miss// l2_rqsts.all_demand_references
      'LLC-load',
      'LLC-store',
      'dTLB-load',
      'dTLB-store',]
  },
  yAxis: {
    type: 'value'
  },
  series: [
    {
      name: 'Collect(1, 1)',
      data: [
        9237059,
        251477589,
      157029061,
      7347988,
      124924824,
      2453354,
      3525463,
        ],
      type: 'bar'
    },
    {
      name: 'Collect(4, 1)',
      data: [
        18428897,
        262577326,
        165034985,
        7168192,
        94839030,
        2133153,
        5870584,
        ],
      type: 'bar'
    },
    {
      name: 'Collect(1, 4)',
      data: [
        13583128,
        266292133,
        170453160,
        7561153,
        109869698,
        3191043,
        3602254,
        ],
      type: 'bar'
    },
    {
      name: 'Collect(3, 4)',
      data: [
        14695808,
        264499881,
       175334268,
        7445770,
        126054487,
        3366703,
        2694442,
        ],
      type: 'bar'
    },
    {
      name: 'Sequential',
      data: [
        1028461,
        175389012,
        123387834,
        4486374,
        108226402,
        705131,
        2208577,
        ],
      type: 'bar'
    }
  ]
};