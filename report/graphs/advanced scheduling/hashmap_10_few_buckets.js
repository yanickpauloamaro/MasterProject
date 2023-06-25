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
        data: [{ yAxis: 5.517 }],
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
      data: [5.970, 9.892, 12.720, 14.088, 14.076, 13.935],
    },
    { 
      type: 'line', 
      symbolSize: '10',
      lineStyle: { width: 3 },
      name: '2 schedulers',
      data: [6.343, 10.841, 13.708, 14.564, 15.230, 15.395],
    },
    {
      type: 'line', 
      symbolSize: '10',
      lineStyle: { width: 3 },
      name: '4 schedulers',
      data: [6.360, 11.000, 14.284, 16.254, 16.765, 15.853],
    },
    {
      type: 'line', 
      symbolSize: '10',
      lineStyle: { width: 3 },
      name: '8 schedulers',
      data: [6.365, 10.820, 14.124, 15.511, 16.004, 13.882],
    },
  ];
  
  option = {
    title: {
      text: 'Hashmap 10% (64 buckets, advanced scheduling)',
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