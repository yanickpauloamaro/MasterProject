var series = [
    // Hashmap 50%, basic scheduling (4 schedulers)
    {
      type: 'bar', name: 'scheduling', stack: 'breakdown', areaStyle: {},
      data: [ 1427, 1425, 1454, 1443, 1606, 1605 ]
    },
    {
      type: 'bar', name: 'execution', stack: 'breakdown', areaStyle: {},
      data: [7353, 3882, 2028, 1641, 1275, 1087 ]
    },
    {
      type: 'bar', name: 'other', stack: 'breakdown',
      data: [ 9402 - 1427 - 7353, 6269 - 1425 - 3882, 5549 - 1454 - 2028, 6265 - 1443 - 1641, 7290 - 1606 - 1275, 7994 - 1605 - 1087,]
    },
    {
      type: 'line',
      data: [10100],
      symbol: 'none'
    }
];

option = {
  legend: {
    right: '0%',
    // bottom: '50%',
    orient: 'vertical',
    textStyle: { fontSize: 22, },
  },
  textStyle: {
    fontSize: 20,
  },
  toolbox: {
    feature: {
      saveAsImage: {}
    }
  },
  title: {
    text: 'Hashmap 50% (basic scheduling)',
    left: 'center',
    textStyle: { fontSize: '24' }
  },
  grid: {
    // right: 150
    left: '12%'
  },
  tooltip: { trigger: 'axis'},
  xAxis: {
    type: 'category',
    data: ['2 cores', '4 cores', '8 cores', '12 cores', '16 cores', '20 cores'],
    name: 'Number of execution cores',
    nameLocation: 'middle',
    nameGap: 40,
    nameTextStyle: { fontSize: 24, },
    axisLabel: { fontSize: 24 },
  },
  yAxis: {
    // max: 2100,
    type: 'value',
    name: 'Latency',
    nameTextStyle: { fontSize: 24, },
    axisLabel: {
      formatter: '{value} µs',
      fontSize: 22,
    },
    
  },
  series: series
};
