let all_core_values = [
    ['Core#0', 2.282, 2.215, 2.198],
    ['Core#1', 2.218, 2.205, 2.200],
    ['Core#2', 2.195, 2.201, 2.194],
    ['Core#3', 2.229, 2.179, 2.200]
  ];
let sizes_str = ['4KB', '8KB', '16KB'];
let sizes = [4000, 8000, 16000];

let l1_size = 49152;
let l2_size = 1310720;
let l3_size = 56623104;


let size_serie = function() {
  return { type: 'line', datasetIndex: 1, xAxisIndex: 0, yAxisIndex: 0, };
};

let size_serie_with_min_max= function() {
  return { 
    type: 'line', datasetIndex: 1, xAxisIndex: 0, yAxisIndex: 0,
    markLine: {
        data: [
          { type: 'max', name: 'Max' },
          { type: 'min', name: 'Min' }
        ]
    }
  };
}

let cache_line = function(name, size_kb) {
    return {
      name: name, data: [], type: 'line',
      xAxisIndex: 1, yAxisIndex: 1,
      markLine: {
        data: [{ xAxis: size_kb }],
        label: { formatter: name},
      },
    };
  };
let zip = function(a, b) {
    return a.map(function(e, i) {
        return [e, b[i]];
      });
}
let core_serie = function(sizes, core_values) {
    let [core, ...values] = core_values;
    // let core = core_values[0];
    // let values = core_values.slice(1);
  return {
    name: core,
    xAxisIndex: 1, yAxisIndex: 1,
    // data: [[4000, 2.218], [8000, 2.205], [16000, 2.200]],
    data: zip(sizes, values),
    type: 'line',
  };
}

let make_dataset = function(sizes_str, core_values) {
  let dimensions = ['memory_core', ...sizes_str];
  let source = [
    dimensions,
    ...core_values
  ];

  return [
    {
      source: source
    },
    {
        transform: {
          type: 'sort',
          fromDatasetIndex: 0,
          config: { dimension: 'memory_core', order: 'asc' }
          // config: { dimension: '8KB', order: 'asc' }
        }
    }
  ];
};

let series = [];
let legend_down = [];

sizes.forEach(size => {
  series.push(size_serie());
})
// Can add _with_min_max for some sizes
series[13] = size_serie_with_min_max();
// series[14] = size_serie_with_min_max();
series[15] = size_serie_with_min_max();

series.push(cache_line('L1', l1_size));
series.push(cache_line('L2', l2_size));
series.push(cache_line('L3', l3_size));

all_core_values.forEach(core_values => {
  legend_down.push(core_values[0]);
  series.push(core_serie(sizes, core_values))
})


option = {
  toolbox: {
    feature: {
      saveAsImage: {}
    }
  },
  legend: [
    { data: sizes_str },
    { data: legend_down, bottom: '0%'},
    ],
  tooltip: {},
  dataset: make_dataset(sizes_str, all_core_values),
  grid: [{ bottom: '55%' }, { bottom: '10%',top: '50%' }],
  xAxis: [
    { type: 'category', gridIndex: 0 },
    { type: 'log', gridIndex: 1 }
  ],
  yAxis: [
    { type: 'value', gridIndex: 0 },
    { type: 'value', gridIndex: 1 }
  ],
  series: series
};