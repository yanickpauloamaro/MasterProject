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
      type: 'line', 
      symbolSize: '10',
      lineStyle: { width: 3 },
      name: 'Sequential',
      data: [ 51.20, 48.76, 40.76, 23.17, 15.78, ], // Transfer 0%
    },
    { 
      type: 'line', 
      symbolSize: '10',
      lineStyle: { width: 3 },
      name: 'basic (8 sch + 8 exec)',
      data: [ 48.33, 72.98, 89.41, 92.43, 64.86,], // Transfer 0% (8 schedulers, 8 cores)
    },
    { 
      type: 'line', 
      symbolSize: '10',
      lineStyle: { width: 3 },
      name: 'advanced (8 sch + 8 exec)',
      data: [ 24.90, 30.26, 32.08, 32.10, 25.58,], // Transfer 0% (8 schedulers, 8 cores)
    },
  ];
  
  option = {
    title: {
      text: 'Transfer 0%'
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
      data: ['16384', '32768', '65536', '131072', '262144'],
      name: 'Batch size',
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