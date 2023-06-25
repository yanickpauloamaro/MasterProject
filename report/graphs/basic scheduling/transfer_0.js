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
        data: [{ yAxis: 32.573 }],
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
      name: '1 schedulers',
      data: [18.306, 23.910, 25.333, 24.796, 24.852, 24.947]
    },
    { 
      type: 'line', 
      symbolSize: '10',
      lineStyle: { width: 3 },
      name: '2 schedulers',
      data: [33.781, 42.091, 46.745, 46.845, 46.055, 46.745]
    },
    {
      type: 'line', 
      symbolSize: '10',
      lineStyle: { width: 3 },
      name: '4 schedulers',
      data: [48.871, 63.875, 69.868, 73.636, 72.576, 71.235]
    },
    {
      type: 'line', 
      symbolSize: '10',
      lineStyle: { width: 3 },
      name: '8 schedulers',
      data: [51.603, 70.926, 79.150, 89.043, 78.205, 63.320]
    },
  ];
  
  option = {
    title: {
      text: 'Transfer 0% conflict',
      textStyle:  { fontSize: 20, },
    },
    toolbox: {
      feature: {
        saveAsImage: {}
      }
    },
    legend: {
      right: '0%',
      // top: '25%',
      orient: 'vertical',
      textStyle:  { fontSize: 20, },
    },
    grid: {
      right: '10%', 
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