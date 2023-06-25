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
        data: [{ yAxis: 5.443 }],
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
      data: [2.273, 2.475, 2.587, 2.607, 2.567, 2.554],
    },
    { 
      type: 'line', 
      symbolSize: '10',
      lineStyle: { width: 3 },
      name: '2 schedulers',
      data: [5.952, 7.976, 8.785, 8.522, 7.826, 7.386],
    },
    {
      type: 'line', 
      symbolSize: '10',
      lineStyle: { width: 3 },
      name: '4 schedulers',
      data: [6.970, 10.454, 11.810, 10.461, 8.990, 8.198],
    },
    {
      type: 'line', 
      symbolSize: '10',
      lineStyle: { width: 3 },
      name: '8 schedulers',
      data: [6.661, 9.224, 10.153, 8.739, 7.482, 6.486],
    },
  ];
  
  option = {
    title: {
      text: 'Hashmap 50% update',
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