let transfer_0 = [
    {
      // 8 schedulers
      type: 'bar', name: 'scheduling', stack: 'breakdown-transfer-0', areaStyle: {},
      data: [ 213, 213, 228, 196, 195, 191, ]
    },
    {
      type: 'bar', name: 'execution', stack: 'breakdown-transfer-0', areaStyle: {},
      data: [ 799, 405, 269, 262, 206, 142, ]
    },
    {
      type: 'bar', name: 'other', stack: 'breakdown-transfer-0',
      data: [ 1274 - 213 - 799, 899 - 213 - 405, 766 - 228 - 269, 781 - 196 - 262, 873 - 195 - 206, 1156 - 191 - 142,]
    },
];

let transfer_50 = [
    {
      // 4 schedulers
      type: 'bar', name: 'scheduling', stack: 'breakdown-transfer-50', areaStyle: {},
      data: [ 503, 555, 613, 670, 509, 493, ]
    },
    {
      type: 'bar', name: 'execution', stack: 'breakdown-transfer-50', areaStyle: {},
      data: [ 787, 450, 272, 187, 273, 175, ]
    },
    {
      type: 'bar', name: 'other', stack: 'breakdown-transfer-50',
      data: [ 1506 - 503 - 787, 1305 - 555 - 450, 1269 - 613 - 272, 1342 - 670 - 187, 1654 - 509 - 273, 1903 - 493 - 175,]
    },
];

let hashmap_10 = [
    {
      // 2 schedulers
      type: 'bar', name: 'scheduling', stack: 'breakdown-hashmap-10', areaStyle: {},
      data: [ 1173, 1158, 1155, 1173, 1323, 1280, ]
    },
    {
      type: 'bar', name: 'execution', stack: 'breakdown-hashmap-10', areaStyle: {},
      data: [ 5633, 2847, 1433, 1095, 882, 772, ]
    },
    {
      type: 'bar', name: 'other', stack: 'breakdown-hashmap-10',
      data: [ 7131 - 1173 - 5633, 4592 - 1158 - 2847, 3540 - 1155 - 1433, 3615 - 1173 - 1095, 3795 - 1323 - 882, 4270 - 1280 - 772,]
    },
];

let hashmap_50 = [
    {
      // 4 schedulers
      type: 'bar', name: 'scheduling', stack: 'breakdown-hashmap-50', areaStyle: {},
      data: [ 1605, 1816, 1864, 1928, 1692, 1603, ]
    },
    {
      type: 'bar', name: 'execution', stack: 'breakdown-hashmap-50', areaStyle: {},
      data: [ 7462, 4003, 2049, 1408, 1442, 1052, ]
    },
    {
      type: 'bar', name: 'other', stack: 'breakdown-hashmap-50',
      data: [ 9612 - 1605 - 7462, 6869 - 1816 - 4003, 5686 - 1864 - 2049, 6238 - 1928 - 1408, 7481 - 1692 - 1442, 8836 - 1603 - 1052,]
    },
];

let fib = [
    {
      // 8 schedulers
      type: 'bar', name: 'scheduling', stack: 'breakdown', areaStyle: {},
      data: [ 159, 167, 162, 158, null, null, ]
    },
    {
      type: 'bar', name: 'execution', stack: 'breakdown', areaStyle: {},
      data: [ 707654, 353645, 176877, 207146, null, null, ]
    },
    {
      type: 'bar', name: 'other', stack: 'breakdown',
      data: [ 708436 - 159 - 707654, 354165 - 167 - 353645, 177388 - 162 - 176877, 207908 - 158 - 207146,]
    },
];

// let title = 'Transfer (no conflicts)'
// let series = transfer_0;

// let title = 'Transfer (50% conflicts)'
// let series = transfer_50;

// let title = 'Hashmap (10% updates)'
// let series = hashmap_10;

let title = 'Hashmap (50% updates)'
let series = hashmap_50;

// let title = 'Fibpnacci'
// let series = fib;

option = {
  legend: {
    left: '85%',
    bottom: '50%',
    orient: 'vertical',
  },
  textStyle: {
    fontSize: 20,
  },
  toolbox: {
    feature: {
      saveAsImage: {}
    }
  },
  title: {
    text: title,
    left: 'center',
    textStyle: { fontSize: '26' }
  },
  grid: {
    right: 150
  },
  tooltip: { trigger: 'axis'},
  xAxis: {
    type: 'category',
    data: ['2 cores', '4 cores', '8 cores', '12 cores', '16 cores', '22 cores'],
    axisLabel: { fontSize: 20 },
  },
  yAxis: {
    type: 'value',
    name: 'Latency',
    axisLabel: {
      formatter: '{value} µs',
      fontSize: 20,
    },
    
  },
  series: [
    ...series,
  ]
};