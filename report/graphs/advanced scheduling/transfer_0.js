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
        data: [{ yAxis:  37.817 }],
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
        data: [12.225, 13.258, 13.710, 13.899, 13.902, 13.673],
    },
    { 
        type: 'line', 
        symbolSize: '10',
        lineStyle: { width: 3 },
        name: '2 schedulers',
        data: [19.118, 22.383, 23.498, 23.901, 23.557, 23.574],
    },
    {
        type: 'line', 
        symbolSize: '10',
        lineStyle: { width: 3 },
        name: '4 schedulers',
        data: [28.188, 31.629, 35.102, 35.910, 33.132, 31.969],
    },
    {
        type: 'line', 
        symbolSize: '10',
        lineStyle: { width: 3 },
        name: '8 schedulers',
        data: [33.729, 37.838, 42.091, 41.796, 39.647, 36.389],
    },
];
  
option = {
    title: { text: 'Transfer 0% (adv)' },
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