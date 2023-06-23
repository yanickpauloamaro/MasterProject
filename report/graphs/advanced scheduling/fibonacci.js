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
        data: [{ yAxis: 0.046 }],
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
      data: [0.093, 0.185, 0.368, 0.551, 0.731, 0.522],
    },
    { 
      type: 'line', 
      symbolSize: '10',
      lineStyle: { width: 3 },
      name: '2 schedulers',
      data: [0.093, 0.185, 0.369, 0.551, 0.732, 0.523],
    },
    {
      type: 'line', 
      symbolSize: '10',
      lineStyle: { width: 3 },
      name: '4 schedulers',
      data: [0.093, 0.185, 0.369, 0.551, 0.732, 0.523],
    },
    {
      type: 'line', 
      symbolSize: '10',
      lineStyle: { width: 3 },
      name: '8 schedulers',
      data: [0.093, 0.185, 0.369, 0.551, 0.732, 0.523],
    },
  ];
  
  option = {
    title: {
      text: 'Fibonacci 20'
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