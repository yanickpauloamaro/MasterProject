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
        // data: [40.010],
        symbolSize: 0,
        markLine: {
        data: [{ yAxis:  5.989 }],
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
        data: [7.139, 10.121, 12.681, 14.073, 15.000, 14.486],
    },
    { 
        type: 'line', 
        symbolSize: '10',
        lineStyle: { width: 3 },
        name: '2 schedulers',
        data: [7.547, 11.063, 13.399, 15.735, 16.110, 15.739],
    },
    {
        type: 'line', 
        symbolSize: '10',
        lineStyle: { width: 3 },
        name: '4 schedulers',
        data: [7.826, 11.868, 14.905, 16.182, 17.134, 16.425],
    },
    {
        type: 'line', 
        symbolSize: '10',
        lineStyle: { width: 3 },
        name: '8 schedulers',
        data: [7.718, 11.212, 13.917, 14.632, 14.966, 13.650],
    },
];
  
option = {
    title: { text: 'Hashmap 10% (adv)' },
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