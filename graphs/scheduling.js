let latency_parallelism = true;
let workload = 'Transfer(0.0)';
// let workload = 'Transfer(0.5)';
// let workload = 'DHashMap';

let data = {
  datasetId: 'data',
  source: [
// Transfer(0.0): =======================================================================
{workload: 'Transfer(0.0)', scheduling: 'basic', size: 65536, parallelism: 0.000, latency: 143, iter: 1 },
{workload: 'Transfer(0.0)', scheduling: 'basic', size: 32768, parallelism: 0.000, latency: 59, iter: 1 },
{workload: 'Transfer(0.0)', scheduling: 'basic', size: 16384, parallelism: 0.000, latency: 29, iter: 1 },

{workload: 'Transfer(0.0)', scheduling: 'address', size: 65536, parallelism: 1.000, latency: 2554, iter: 1 },
{workload: 'Transfer(0.0)', scheduling: 'address', size: 32768, parallelism: 1.000, latency: 963, iter: 1 },
{workload: 'Transfer(0.0)', scheduling: 'address', size: 16384, parallelism: 1.000, latency: 434, iter: 1 },

{workload: 'Transfer(0.0)', scheduling: 'read/write', size: 65536, parallelism: 1.000, latency: 4899, iter: 1 },
{workload: 'Transfer(0.0)', scheduling: 'read/write', size: 32768, parallelism: 1.000, latency: 1872, iter: 1 },
{workload: 'Transfer(0.0)', scheduling: 'read/write', size: 16384, parallelism: 1.000, latency: 524, iter: 1 },

{workload: 'Transfer(0.0)', scheduling: 'assign(1)', size: 65536, parallelism: 1.000, latency: 4663, iter: 1 },
{workload: 'Transfer(0.0)', scheduling: 'assign(1)', size: 32768, parallelism: 1.000, latency: 2071, iter: 1 },
{workload: 'Transfer(0.0)', scheduling: 'assign(1)', size: 16384, parallelism: 1.000, latency: 909, iter: 1 },
{workload: 'Transfer(0.0)', scheduling: 'assign(2)', size: 65536, parallelism: 1.000, latency: 4859, iter: 1 },
{workload: 'Transfer(0.0)', scheduling: 'assign(2)', size: 32768, parallelism: 1.000, latency: 2176, iter: 1 },
{workload: 'Transfer(0.0)', scheduling: 'assign(2)', size: 16384, parallelism: 1.000, latency: 922, iter: 1 },
{workload: 'Transfer(0.0)', scheduling: 'assign(4)', size: 65536, parallelism: 1.000, latency: 5068, iter: 1 },
{workload: 'Transfer(0.0)', scheduling: 'assign(4)', size: 32768, parallelism: 1.000, latency: 2272, iter: 1 },
{workload: 'Transfer(0.0)', scheduling: 'assign(4)', size: 16384, parallelism: 1.000, latency: 952, iter: 1 },


// Transfer(0.5): =======================================================================
{workload: 'Transfer(0.5)', scheduling: 'basic', size: 65536, parallelism: 0.000, latency: 129, iter: 1 },
{workload: 'Transfer(0.5)', scheduling: 'basic', size: 32768, parallelism: 0.000, latency: 59, iter: 1 },
{workload: 'Transfer(0.5)', scheduling: 'basic', size: 16384, parallelism: 0.000, latency: 29, iter: 1 },

{workload: 'Transfer(0.5)', scheduling: 'address', size: 65536, parallelism: 0.721, latency: 2854, iter: 1 },
{workload: 'Transfer(0.5)', scheduling: 'address', size: 32768, parallelism: 0.844, latency: 1129, iter: 1 },
{workload: 'Transfer(0.5)', scheduling: 'address', size: 16384, parallelism: 0.918, latency: 471, iter: 1 },
{workload: 'Transfer(0.5)', scheduling: 'address', size: 65536, parallelism: 0.942, latency: 3262, iter: 2 },
{workload: 'Transfer(0.5)', scheduling: 'address', size: 32768, parallelism: 0.981, latency: 1193, iter: 2 },
{workload: 'Transfer(0.5)', scheduling: 'address', size: 16384, parallelism: 0.995, latency: 481, iter: 2 },
{workload: 'Transfer(0.5)', scheduling: 'address', size: 65536, parallelism: 0.987, latency: 3485, iter: 3 },
{workload: 'Transfer(0.5)', scheduling: 'address', size: 32768, parallelism: 0.998, latency: 1228, iter: 3 },
{workload: 'Transfer(0.5)', scheduling: 'address', size: 16384, parallelism: 1.000, latency: 499, iter: 3 },

{workload: 'Transfer(0.5)', scheduling: 'read/write', size: 65536, parallelism: 0.721, latency: 4236, iter: 1 },
{workload: 'Transfer(0.5)', scheduling: 'read/write', size: 32768, parallelism: 0.844, latency: 1688, iter: 1 },
{workload: 'Transfer(0.5)', scheduling: 'read/write', size: 16384, parallelism: 0.918, latency: 556, iter: 1 },
{workload: 'Transfer(0.5)', scheduling: 'read/write', size: 65536, parallelism: 0.942, latency: 5955, iter: 2 },
{workload: 'Transfer(0.5)', scheduling: 'read/write', size: 32768, parallelism: 0.981, latency: 1743, iter: 2 },
{workload: 'Transfer(0.5)', scheduling: 'read/write', size: 16384, parallelism: 0.995, latency: 577, iter: 2 },
{workload: 'Transfer(0.5)', scheduling: 'read/write', size: 65536, parallelism: 0.987, latency: 5230, iter: 3 },
{workload: 'Transfer(0.5)', scheduling: 'read/write', size: 32768, parallelism: 0.998, latency: 1717, iter: 3 },
{workload: 'Transfer(0.5)', scheduling: 'read/write', size: 16384, parallelism: 1.000, latency: 566, iter: 3 },

{workload: 'Transfer(0.5)', scheduling: 'assign(1)', size: 65536, parallelism: 1.000, latency: 6578, iter: 1 },
{workload: 'Transfer(0.5)', scheduling: 'assign(1)', size: 32768, parallelism: 1.000, latency: 2341, iter: 1 },
{workload: 'Transfer(0.5)', scheduling: 'assign(1)', size: 16384, parallelism: 1.000, latency: 984, iter: 1 },
{workload: 'Transfer(0.5)', scheduling: 'assign(2)', size: 65536, parallelism: 1.000, latency: 6733, iter: 1 },
{workload: 'Transfer(0.5)', scheduling: 'assign(2)', size: 32768, parallelism: 1.000, latency: 2417, iter: 1 },
{workload: 'Transfer(0.5)', scheduling: 'assign(2)', size: 16384, parallelism: 1.000, latency: 973, iter: 1 },
{workload: 'Transfer(0.5)', scheduling: 'assign(4)', size: 65536, parallelism: 1.000, latency: 6916, iter: 1 },
{workload: 'Transfer(0.5)', scheduling: 'assign(4)', size: 32768, parallelism: 1.000, latency: 2429, iter: 1 },
{workload: 'Transfer(0.5)', scheduling: 'assign(4)', size: 16384, parallelism: 1.000, latency: 969, iter: 1 },

// DHashMap: ============================================================================
{workload: 'DHashMap', scheduling: 'basic', size: 65536, parallelism: 0.498, latency: 758, iter: 1 },
{workload: 'DHashMap', scheduling: 'basic', size: 32768, parallelism: 0.500, latency: 336, iter: 1 },
{workload: 'DHashMap', scheduling: 'basic', size: 16384, parallelism: 0.503, latency: 147, iter: 1 },

{workload: 'DHashMap', scheduling: 'address', size: 65536, parallelism: 0.498, latency: 930, iter: 1 },
{workload: 'DHashMap', scheduling: 'address', size: 32768, parallelism: 0.500, latency: 443, iter: 1 },
{workload: 'DHashMap', scheduling: 'address', size: 16384, parallelism: 0.503, latency: 215, iter: 1 },
{workload: 'DHashMap', scheduling: 'address', size: 65536, parallelism: 0.498, latency: 1538, iter: 2 },
{workload: 'DHashMap', scheduling: 'address', size: 32768, parallelism: 0.500, latency: 745, iter: 2 },
{workload: 'DHashMap', scheduling: 'address', size: 16384, parallelism: 0.503, latency: 325, iter: 2 },
{workload: 'DHashMap', scheduling: 'address', size: 65536, parallelism: 0.498, latency: 2121, iter: 3 },
{workload: 'DHashMap', scheduling: 'address', size: 32768, parallelism: 0.500, latency: 1063, iter: 3 },
{workload: 'DHashMap', scheduling: 'address', size: 16384, parallelism: 0.503, latency: 445, iter: 3 },
{workload: 'DHashMap', scheduling: 'address', size: 65536, parallelism: 0.498, latency: 2729, iter: 4 },
{workload: 'DHashMap', scheduling: 'address', size: 32768, parallelism: 0.500, latency: 1287, iter: 4 },
{workload: 'DHashMap', scheduling: 'address', size: 16384, parallelism: 0.503, latency: 576, iter: 4 },
{workload: 'DHashMap', scheduling: 'address', size: 65536, parallelism: 0.498, latency: 3283, iter: 5 },
{workload: 'DHashMap', scheduling: 'address', size: 32768, parallelism: 0.501, latency: 1494, iter: 5 },
{workload: 'DHashMap', scheduling: 'address', size: 16384, parallelism: 0.503, latency: 720, iter: 5 },

{workload: 'DHashMap', scheduling: 'read/write', size: 65536, parallelism: 0.514, latency: 2012, iter: 1 },
{workload: 'DHashMap', scheduling: 'read/write', size: 32768, parallelism: 0.532, latency: 867, iter: 1 },
{workload: 'DHashMap', scheduling: 'read/write', size: 16384, parallelism: 0.565, latency: 448, iter: 1 },
{workload: 'DHashMap', scheduling: 'read/write', size: 65536, parallelism: 0.529, latency: 3592, iter: 2 },
{workload: 'DHashMap', scheduling: 'read/write', size: 32768, parallelism: 0.563, latency: 1623, iter: 2 },
{workload: 'DHashMap', scheduling: 'read/write', size: 16384, parallelism: 0.628, latency: 785, iter: 2 },
{workload: 'DHashMap', scheduling: 'read/write', size: 65536, parallelism: 0.545, latency: 4997, iter: 3 },
{workload: 'DHashMap', scheduling: 'read/write', size: 32768, parallelism: 0.594, latency: 2154, iter: 3 },
{workload: 'DHashMap', scheduling: 'read/write', size: 16384, parallelism: 0.689, latency: 985, iter: 3 },
{workload: 'DHashMap', scheduling: 'read/write', size: 65536, parallelism: 0.560, latency: 6335, iter: 4 },
{workload: 'DHashMap', scheduling: 'read/write', size: 32768, parallelism: 0.625, latency: 2857, iter: 4 },
{workload: 'DHashMap', scheduling: 'read/write', size: 16384, parallelism: 0.749, latency: 1197, iter: 4 },
{workload: 'DHashMap', scheduling: 'read/write', size: 65536, parallelism: 0.576, latency: 7606, iter: 5 },
{workload: 'DHashMap', scheduling: 'read/write', size: 32768, parallelism: 0.657, latency: 3527, iter: 5 },
{workload: 'DHashMap', scheduling: 'read/write', size: 16384, parallelism: 0.805, latency: 1351, iter: 5 },

{workload: 'DHashMap', scheduling: 'assign(1)', size: 65536, parallelism: 1.000, latency: 2266, iter: 1 },
{workload: 'DHashMap', scheduling: 'assign(1)', size: 32768, parallelism: 1.000, latency: 1114, iter: 1 },
{workload: 'DHashMap', scheduling: 'assign(1)', size: 16384, parallelism: 1.000, latency: 509, iter: 1 },
{workload: 'DHashMap', scheduling: 'assign(2)', size: 65536, parallelism: 1.000, latency: 2243, iter: 1 },
{workload: 'DHashMap', scheduling: 'assign(2)', size: 32768, parallelism: 1.000, latency: 1114, iter: 1 },
{workload: 'DHashMap', scheduling: 'assign(2)', size: 16384, parallelism: 1.000, latency: 529, iter: 1 },
{workload: 'DHashMap', scheduling: 'assign(4)', size: 65536, parallelism: 1.000, latency: 2263, iter: 1 },
{workload: 'DHashMap', scheduling: 'assign(4)', size: 32768, parallelism: 1.000, latency: 1120, iter: 1 },
{workload: 'DHashMap', scheduling: 'assign(4)', size: 16384, parallelism: 1.000, latency: 535, iter: 1 },

  ]
};

let config = {
  encode_y: 'latency'
};

let make_dataset = (scheduling, iter, size = null) => {
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
      id: scheduling,
      transform: transforms
    };
}

if (latency_parallelism) {
  config.iter = null;
  config.size = 65536;
  config.show_point_label = true;
  config.serie_type = 'scatter';
  config.point_label = 'iter';
  config.encode_x = 'parallelism',
  config.xAxis = {
    // inverse: true,
    name: 'Parallelism',
    nameLocation: 'middle'
  };
} else {
  config.iter = 1;
  config.size = null;
  config.show_point_label = false;
  config.serie_type = 'line';
  config.encode_x = 'size',
  config.xAxis = {
    inverse: true,
    name: 'batch size',
    nameLocation: 'middle'
  };
}

config.datasets = [
    make_dataset('basic', config.iter, config.size),
    make_dataset('address', config.iter, config.size),
    make_dataset('read/write', config.iter, config.size),
    make_dataset('assign(1)', config.iter, config.size),
    make_dataset('assign(2)', config.iter, config.size),
    make_dataset('assign(4)', config.iter, config.size),
    ]

let make_serie = (name, config) => {
  return {
      name: name,
      symbolSize: 20,
      datasetId: name,
      type: config.serie_type,
      encode: { x: config.encode_x, y: config.encode_y},
      label: {
        show: config.show_point_label,
        formatter: function (param) {
          return param.data[config.point_label];
        },
      }
    };
};

option = {
  dataset: [
    data,
    ...config.datasets
  ],
  legend: {},
  tooltip: {
    trigger: 'axis',
    valueFormatter: (value) => value + ' µs',
  },
  xAxis: config.xAxis,
  yAxis: {
    name: 'Latency',
    axisLabel: {
      formatter: '{value} µs'
    }
  },
  series: [
    make_serie('basic', config),
    make_serie('address', config),
    make_serie('read/write', config),
    // make_serie('assign(1)', config),
    // make_serie('assign(2)', config),
    make_serie('assign(4)', config),
  ]
};