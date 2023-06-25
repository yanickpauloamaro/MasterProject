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
        data: [{ yAxis:  5.247 }],
        symbolSize: '10',
        symbol: 'none',
        lineStyle: { width: 3 },
        label: {
            // formatter: 'Sequential'
            fontSize: 20
        },
        },
    },
    {
      type: 'line', 
      symbolSize: '10',
      lineStyle: { width: 3 },
      name: 'basic scheduling (4 sch)',
      data: [6.970, 10.454, 11.810, 10.461, 8.990, 8.198],
    },
    {
        type: 'line', 
        symbolSize: '10',
        lineStyle: { width: 3 },
        name: 'advanced scheduling (4 sch)',
        data: [6.498, 9.502, 12.635, 13.994, 14.173, 13.938],
    },
];
  
option = {
    title: {
        text: 'Hashmap 50% (comparison)',
        textStyle:  { fontSize: 20, },
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