let point_details = {
  symbolSize: '0',
  // itemStyle: {
  //   color: 'rgb(255, 0, 0)',
  //   // opacity: 0
  // },
  label: {
    position: 'top',
    fontWeight: 'bold',
    color: 'inherit'
  }
  // symbolOffset: [3, -20],
}
let line_details = {
  symbolSize: '10', lineStyle: { width: 3 },
}

let series = [
  { 
    type: 'line', 
    symbolSize: '10',
    lineStyle: { width: 3 },
    name: 'Sequential',
    data: [49.35, 46.35, 39.38, 24.66, 16.08], // Transfer 50%
  },
  { 
    type: 'line', 
    symbolSize: '10',
    lineStyle: { width: 3 },
    name: 'basic (8 sch + 8 exec)',
    data: [21.76, 36.90, 51.68, 67.32, 57.77], // Transfer 50% (8 schedulers, 8 cores)
  },
  { 
    type: 'line', 
    symbolSize: '10',
    lineStyle: { width: 3 },
    name: 'advanced (8 sch + 8 exec)',
    data: [15.63, 24.08, 28.67, 32.58, 25.76,], // Transfer 50% (8 schedulers, 8 cores)
  },
];

option = {
  title: {
    text: 'Transfer 50%'
  },
  toolbox: {
    feature: {
      saveAsImage: {}
    }
  },
  legend: {
    textStyle:  { fontSize: 16, },
  },
  tooltip: { trigger: 'axis'},
  xAxis: {
    type: 'category',
    data: ['16384', '32768', '65536', '131072', '262144'],
    name: 'Batch size',
    nameLocation: 'middle',
    nameGap: 40,
    nameTextStyle:  { fontSize: 20, },
    axisLabel: { fontSize: 20 },
  },
  yAxis: {
    type: 'value',
    name: 'Throughput [Million tx/s]',
    nameLocation: 'middle',
    nameGap: 50,
    nameTextStyle:  { fontSize: 20, },
    axisLabel: { fontSize: 20 },
  },
  series: series
};