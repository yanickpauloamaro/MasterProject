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
      name: 'basic scheduling (8 sch)',
      data: [51.603, 70.926, 79.150, 89.043, 78.205, 63.320]
    },
    {
      type: 'line', 
      symbolSize: '10',
      lineStyle: { width: 3 },
      name: 'advanced scheduling (8 sch)',
      data: [33.729, 37.838, 42.091, 41.796, 39.647, 36.389],
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