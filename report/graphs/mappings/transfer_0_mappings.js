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
        data: [{ yAxis:  41.037 }],
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
        data: [34.878, 42.528, 46.185],
    },
    {
        type: 'line', 
        symbolSize: '10',
        lineStyle: { width: 3, type: 'dashed' },
        name: '4 schedulers',
        data: [48.330, 63.442, 74.052],
    },
    {
        type: 'line', 
        symbolSize: '10',
        lineStyle: { width: 3, type: 'dashed' },
        name: '8 schedulers',
        data: [56.448, 74.220, 89.286],
    },
    // Mapping B ===============================================================
    {
        name: 'Sequential', type: 'line',
        markLine: {
        data: [{ yAxis:  38.825 }],
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
        data: [33.267, 40.731, 44.734],
    },
    {
        type: 'line', 
        symbolSize: '10',
        lineStyle: { width: 3, type: 'dotted' },
        name: '4 schedulers',
        data: [44.552, 58.777, 68.409],
    },
    {
        type: 'line', 
        symbolSize: '10',
        lineStyle: { width: 3, type: 'dotted' },
        name: '8 schedulers',
        data: [52.137, 71.235, 84.563],
    },
    // Mapping C ===============================================================
    {
        name: 'Sequential', type: 'line',
        markLine: {
        data: [{ yAxis:  39.290 }],
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
        data: [34.098, 41.930, 45.733],
    },
    {
        type: 'line', 
        symbolSize: '10',
        lineStyle: { width: 3 },
        name: '4 schedulers',
        data: [48.047, 59.632, 67.913],
    },
    {
        type: 'line', 
        symbolSize: '10',
        lineStyle: { width: 3 },
        name: '8 schedulers',
        data: [53.895, 70.773, 87.033],
    },
];
  
option = {
    title: { text: 'Transfer 0%' },
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