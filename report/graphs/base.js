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
        name: 'Sequential', type: 'line',
        markLine: {
        data: [{ yAxis: 65.536/1.881 }],
        symbolSize: '10',
        symbol: 'circle',
        lineStyle: { width: 3 },
        label: {
            // formatter: 'Sequential'
            fontSize: 16
        },
        },
    },
    { 
        type: 'line', 
        symbolSize: '10',
        lineStyle: { width: 3 },
        name: '1 schedulers',
        // data: [65.536/1.99, 65.536/1.612, 65.536/1.411, 65.536/1.411, 65.536/1.451, 65.536/1.476,]
    },
    { 
        type: 'line', 
        symbolSize: '10',
        lineStyle: { width: 3 },
        name: '2 schedulers',
        // data: [65.536/1.99, 65.536/1.612, 65.536/1.411, 65.536/1.411, 65.536/1.451, 65.536/1.476,]
    },
    {
        type: 'line', 
        symbolSize: '10',
        lineStyle: { width: 3 },
        name: '4 schedulers',
        // data: [65.536/1.394, 65.536/1.077, 65.536/0.897, 65.536/0.807, 65.536/0.946, 65.536/0.980,]
    },
    {
        type: 'line', 
        symbolSize: '10',
        lineStyle: { width: 3 },
        name: '8 schedulers',
        // data: [65.536/1.274, 65.536/0.899, 65.536/0.766, 65.536/0.781, 65.536/0.873, 65.536/1.156,]
    },
];
  
option = {
    title: { text: 'Transfer 0% conflict' },
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
        data: ['2 cores', '4 cores', '8 cores', '12 cores', '16 cores', '20 cores'],
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