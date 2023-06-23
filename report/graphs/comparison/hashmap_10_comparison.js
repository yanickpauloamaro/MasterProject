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
      name: 'basic scheduling (2 sch)',
      data: [9.348, 14.306, 17.679, 17.896, 17.242, 16.364],
    },
    {
        type: 'line', 
        symbolSize: '10',
        lineStyle: { width: 3 },
        name: 'advanced scheduling (4 sch)',
        data: [7.826, 11.868, 14.905, 16.182, 17.134, 16.425],
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