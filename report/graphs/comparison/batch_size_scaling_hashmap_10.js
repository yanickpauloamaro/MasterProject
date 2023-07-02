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
    data: [ 6.45, 6.23, 6.11, 5.86, 5.63, ] // HashMap 10%
  },
  { 
    type: 'line', 
    symbolSize: '10',
    lineStyle: { width: 3 },
    name: 'basic (8 sch + 8 exec)',
    data: [ 7.03, 10.96, 13.90, 17.12, 18.29, ] // HashMap 10%  (8 schedulers, 8 cores)
  },
  { 
    type: 'line', 
    symbolSize: '10',
    lineStyle: { width: 3 },
    name: 'advanced  (8 sch + 8 exec)',
    data: [12.40,13.62,14.34,11.60,10.33,] // HashMap 10%  (8 schedulers, 8 cores)
  },
];

option = {
  title: {
    text: 'HashMap 10%'
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