var series = [
    // Transfer 0%, advanced scheduling (8 schedulers)
    {
      type: 'bar', name: 'scheduling', stack: 'breakdown', areaStyle: {},
      data: [ 309, 514, 310, 428, 443, 423, ]
    },
    {
      type: 'bar', name: 'execution', stack: 'breakdown', areaStyle: {},
      data: [  832, 468, 318, 244, 193, 159, ]
    },
    {
      type: 'bar', name: 'other', stack: 'breakdown',
      data: [ 1943 - 309 - 832, 1732 - 514 - 468, 1557 - 310 - 318, 1568 - 428 - 244, 1653 - 443 - 193, 1801 - 423 - 159,]
    },
];

option = {
  legend: {
    right: '0%',
    // bottom: '50%',
    orient: 'vertical',
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
    text: 'Transfer 0% (advanced scheduling)',
    left: 'center',
    textStyle: { fontSize: '24' }
  },
  grid: {
    // right: 150
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
