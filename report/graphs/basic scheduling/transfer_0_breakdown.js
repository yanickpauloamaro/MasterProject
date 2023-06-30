var series = [
  // Transfer 0%, basic scheduling (8 schedulers)
    {
      type: 'bar', name: 'scheduling', stack: 'breakdown', areaStyle: {},
      data: [  188, 185, 186, 201, 194, 193, ]
    },
    {
      type: 'bar', name: 'execution', stack: 'breakdown', areaStyle: {},
      data: [ 806, 450, 334, 234, 182, 165, ]
    },
    {
      type: 'bar', name: 'other', stack: 'breakdown',
      data: [  1270 - 188 - 806, 924 - 185 - 450, 828 - 186 - 334, 736 - 201 - 234, 838 - 194 - 182, 1035 - 193 - 165, ]
    },
    {
      type: 'line',
      data: [1900],
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
    text: 'Transfer 0% (basic scheduling)',
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
