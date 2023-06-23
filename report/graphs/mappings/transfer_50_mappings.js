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
        data: [{ yAxis:  40.355 }],
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
        data: [33.386, 37.643, 38.505],
    },
    {
        type: 'line', 
        symbolSize: '10',
        lineStyle: { width: 3, type: 'dashed' },
        name: '4 schedulers',
        data: [44.949, 51.080, 52.639],
    },
    {
        type: 'line', 
        symbolSize: '10',
        lineStyle: { width: 3, type: 'dashed' },
        name: '8 schedulers',
        data: [47.906, 53.850, 51.603],
    },
    // Mapping B ===============================================================
    {
        name: 'Sequential', type: 'line',
        markLine: {
        data: [{ yAxis:  40.554 }],
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
        data: [29.654, 34.276, 36.288],
    },
    {
        type: 'line', 
        symbolSize: '10',
        lineStyle: { width: 3, type: 'dotted' },
        name: '4 schedulers',
        data: [41.610, 47.871, 49.648],
    },
    {
        type: 'line', 
        symbolSize: '10',
        lineStyle: { width: 3, type: 'dotted' },
        name: '8 schedulers',
        data: [46.217, 53.325, 52.555],
    },
    // Mapping C ===============================================================
    {
        name: 'Sequential', type: 'line',
        markLine: {
        data: [{ yAxis:  41.037 }],
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
        data: [31.938, 37.068, 37.535],
    },
    {
        type: 'line', 
        symbolSize: '10',
        lineStyle: { width: 3 },
        name: '4 schedulers',
        data: [45.228, 48.473, 49.127],
    },
    {
        type: 'line', 
        symbolSize: '10',
        lineStyle: { width: 3 },
        name: '8 schedulers',
        data: [47.697, 54.432, 52.429],
    },
];
  
option = {
    title: { text: 'Transfer 50%' },
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