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
        data: [{ yAxis: 3.649 }],
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
      data: [1.330, 1.415, 1.454, 1.459, 1.437, 1.422],
    },
    { 
      type: 'line', 
      symbolSize: '10',
      lineStyle: { width: 3 },
      name: '2 schedulers',
      data: [5.028, 6.980, 7.257, 6.942, 5.916, 5.534],
    },
    {
      type: 'line', 
      symbolSize: '10',
      lineStyle: { width: 3 },
      name: '4 schedulers',
      data: [5.136, 7.230, 8.528, 7.174, 6.172, 5.570],
    },
    {
      type: 'line', 
      symbolSize: '10',
      lineStyle: { width: 3 },
      name: '8 schedulers',
      data: [4.969, 7.036, 7.339, 6.116, 5.327, 4.580],
    },
  ];
  
  option = {
    title: {
      text: 'Hashmap 50% (64 buckets)',
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