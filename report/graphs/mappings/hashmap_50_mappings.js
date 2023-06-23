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
        data: [{ yAxis:  3.984 }],
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
        data: [5.144, 6.979, 7.251],
    },
    {
        type: 'line', 
        symbolSize: '10',
        lineStyle: { width: 3, type: 'dashed' },
        name: '4 schedulers',
        data: [5.157, 7.583, 8.557],
    },
    {
        type: 'line', 
        symbolSize: '10',
        lineStyle: { width: 3, type: 'dashed' },
        name: '8 schedulers',
        data: [4.918, 7.046, 7.006],
    },
    // Mapping B ===============================================================
    {
        name: 'Sequential', type: 'line',
        markLine: {
        data: [{ yAxis:  3.927 }],
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
        data: [4.387, 5.977, 6.523],
    },
    {
        type: 'line', 
        symbolSize: '10',
        lineStyle: { width: 3, type: 'dotted' },
        name: '4 schedulers',
        data: [4.814, 7.035, 8.202],
    },
    {
        type: 'line', 
        symbolSize: '10',
        lineStyle: { width: 3, type: 'dotted' },
        name: '8 schedulers',
        data: [4.770, 6.810, 7.454],
    },
    // Mapping C ===============================================================
    {
        name: 'Sequential', type: 'line',
        markLine: {
        data: [{ yAxis:  4.001 }],
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
        data: [5.021, 7.018, 7.094],
    },
    {
        type: 'line', 
        symbolSize: '10',
        lineStyle: { width: 3 },
        name: '4 schedulers',
        data: [5.119, 7.467, 7.822],
    },
    {
        type: 'line', 
        symbolSize: '10',
        lineStyle: { width: 3 },
        name: '8 schedulers',
        data: [4.943, 6.708, 7.213],
    },
];
  
option = {
    title: { text: 'Hashmap 50% updates' },
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