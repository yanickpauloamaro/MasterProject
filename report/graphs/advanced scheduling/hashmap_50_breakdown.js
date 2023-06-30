var series = [
    // Hashmap 50%, advanced scheduling (4 schedulers)
    {
      type: 'bar', name: 'scheduling', stack: 'breakdown', areaStyle: {},
      data: [ 501, 479, 483, 493, 632, 626,]
    },
    {
      type: 'bar', name: 'execution', stack: 'breakdown', areaStyle: {},
      data: [ 6973, 3737, 2082, 1502, 1187, 1171,]
    },
    {
      type: 'bar', name: 'other', stack: 'breakdown',
      data: [  10085 - 501 - 6973, 6897 - 479 - 3737, 5187 - 483 - 2082, 4683 - 493 - 1502, 4624 - 632 - 1187, 4702 - 626 - 1171,]
    },
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
    text: 'Hashmap 50% (advanced scheduling)',
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
