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
    // Mapping A ===============================================================
    {
        name: 'Sequential', type: 'line',
        markLine: {
        data: [{ yAxis:  5.494 }],
        symbolSize: '10',
        symbol: 'circle',
        lineStyle: { width: 3 },
        label: { fontSize: 16 },
        },
    },
    { 
        type: 'line', 
        symbolSize: '10',
        lineStyle: { width: 3, type: 'dashed' },
        name: '2 schedulers',
        data: [7.709, 11.097, 12.293],
    },
    {
        type: 'line', 
        symbolSize: '10',
        lineStyle: { width: 3, type: 'dashed' },
        name: '4 schedulers',
        data: [7.802, 10.777, 11.394],
    },
    {
        type: 'line', 
        symbolSize: '10',
        lineStyle: { width: 3, type: 'dashed' },
        name: '8 schedulers',
        data: [7.434, 9.643, 10.072],
    },
    // Mapping B ===============================================================
    {
        name: 'Sequential', type: 'line',
        markLine: {
        data: [{ yAxis:  5.485 }],
        symbolSize: '10',
        symbol: 'circle',
        lineStyle: { width: 3, type: 'dotted' },
        label: { fontSize: 16 },
        },
    },
    { 
        type: 'line', 
        symbolSize: '10',
        lineStyle: { width: 3, type: 'dotted' },
        name: '2 schedulers',
        data: [7.484, 10.832, 12.172],
    },
    {
        type: 'line', 
        symbolSize: '10',
        lineStyle: { width: 3, type: 'dotted' },
        name: '4 schedulers',
        data: [7.671, 10.123, 11.524],
    },
    {
        type: 'line', 
        symbolSize: '10',
        lineStyle: { width: 3, type: 'dotted' },
        name: '8 schedulers',
        data: [7.377, 9.747, 10.207],
    },
    // Mapping C ===============================================================
    {
        name: 'Sequential', type: 'line',
        markLine: {
        data: [{ yAxis:  3.927 }],
        symbolSize: '10',
        symbol: 'circle',
        lineStyle: { width: 3, type: 'solid' },
        label: { fontSize: 16 },
        },
    },
    { 
        type: 'line', 
        symbolSize: '10',
        lineStyle: { width: 3 },
        name: '2 schedulers',
        data: [4.387, 5.977, 6.523],
    },
    {
        type: 'line', 
        symbolSize: '10',
        lineStyle: { width: 3 },
        name: '4 schedulers',
        data: [4.814, 7.035, 8.202],
    },
    {
        type: 'line', 
        symbolSize: '10',
        lineStyle: { width: 3 },
        name: '8 schedulers',
        data: [4.770, 6.810, 7.454],
    },
];
  
option = {
    title: { text: 'Hashmap 10% updates' },
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
        data: ['2 cores', '4 cores', '8 cores'],
        name: 'Number of execution cores',
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