let data = [
    ['2 sch',	14.477,	10.738,	10.448,	10.577,	10.762,	10.948,	11.187,	11.87,	11.797],
    ['3 sch',	13.668,	9.325, 	9.489,	9.154,	9.366,	9.578,	9.881,	10.985,	10.52],
    ['4 sch',	11.784,	8.37,  	8.705,	8.399,	8.538,	8.87,	9.165,	9.534,	9.811],
    ['5 sch',	11.592,	8.6,   	8.387,	7.826,	8.081,	9.166,	9.561,	9.344,	9.566],
    ['6 sch',	11.601,	8.278, 	7.488,	7.665,	7.954,	9.323,	9.149,	9.337,	9.788],
    ['8 sch',	11.544,	8.295, 	7.747,	8.396,	8.743,	9.369,	9.727,	10,	    10.369],
    ['10 sch',	11.467,	8.608, 	8.496,	8.846,	9.318,	10.049,	10.973,	10.688,	11.21],
    ['12 sch',	11.535,	9.032, 	9.114,	9.572,	10.133,	10.744,	11.742,	12.45,	12.349],
    ['16 sch',	12.426,	10.425,	10.454,	10.789,	11.709,	12.427,	13.655,	14.785,	16.015],
];
let sequential_latency = 10.398;

// ---------------------------------------------------------------
let dimensions = ['...', '2 exec', '3 exec', '4 exec', '5 exec', '6 exec', '8 exec', '10 exec', '12 exec', '15 exec'];
let top_series = [];
let top_legend = [];
data.forEach(el => {
  top_series.push({
    type: 'line',
    seriesLayoutBy: 'row',
    //tooltip: { formatter: '{a} schedulers' }
  });
  top_legend.push(el[0]);
});

let bottom_series = [];
let bottom_legend = [];
dimensions.forEach((el, i) => {
  if (i != 0) {
    bottom_series.push({ type: 'line', xAxisIndex: 1, yAxisIndex: 1 });
    bottom_legend.push(el);
  }
})

option = {
  legend: [
    { orient: 'vertical', type: 'scroll', data: top_legend,    top: '25%', right: '0%'},
    { orient: 'vertical', type: 'scroll', data: bottom_legend, bottom: '25%', right: '0%'}
    ],
  axisPointer: {
    type: 'line'
  },
  tooltip: {
    /*formatter: (point) => {
      let axis_name = '';
      let series_name = '';
      console.log(point)
      if (point[0].axisIndex == 0) {
        axis_name = point[0].axisValue + ' executors';
        series_name = 'schedulers';
      } else {
        axis_name = point[0].axisValue + ' schedulers';
        series_name = 'executors';
      }

      return axis_name;
    }*/
    valueFormatter: (value) => value + ' ms'
  },
  dataset: {
      source: [
      dimensions,
      ...data
    ]
  },
  xAxis: [
    { id: 0, type: 'category', gridIndex: 0, name: 'number of executors', nameLocation: 'middle', nameGap: 25},
    { id: 1, type: 'category', gridIndex: 1, name: 'number of schedulers', nameLocation: 'middle', nameGap: 25}
  ],
  yAxis: [{ gridIndex: 0 }, { gridIndex: 1 }],
  grid: [
    {
      bottom: '55%',
      tooltip: {
        trigger: 'axis',
        formatter: (point) => {
          console.log('????')
          return 'grid up';
        }
        //valueFormatter: (value) => 'hello'
      }
    },
    {
      top: '55%',
      tooltip: {
        trigger: 'axis',
      }
    }],
  series: [
    {
      name: 'Sequential', data: [], type: 'line',
      xAxisIndex: 1, yAxisIndex: 1,
      markLine: {
        data: [{ yAxis: sequential_latency }],
        label: { formatter: 'Sequential'},
      },
    },
    {
      name: 'Sequential', data: [], type: 'line',
      markLine: {
        data: [{ yAxis: sequential_latency }],
        label: { formatter: 'Sequential'},
      },
    },
    ...top_series,
    ...bottom_series,
  ]
};