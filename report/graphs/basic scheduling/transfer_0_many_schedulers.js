let dimensions = ['...', '4 exec', '8 exec', '12 exec'];
// Workload: Transfer(0.0) ====================================================
let title = 'Collect: Transfer(0.0)'; // --------------------
let sequential_latency = 39.791;
let data = [
	['1 schedulers', 24.454, 25.343, 25.382],
	['2 schedulers', 42.037, 46.414, 46.778],
	['4 schedulers', 62.594, 69.719, 72.899],
	['8 schedulers', 70.697, 79.341, 89.043],
	['10 schedulers', 75.940, 91.531, 90.645],
	['12 schedulers', 76.561, 91.531, 84.672],
	['16 schedulers', 68.768, 80.809, 71.860],
];

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
  title: {
    text: title,
    left: 'center',
  },
  legend: [
    { orient: 'vertical', type: 'scroll', data: top_legend,    top: '10%', right: '0%'},
    { orient: 'vertical', type: 'scroll', data: bottom_legend, top: '60%', right: '0%'}
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
    // valueFormatter: (value) => value + ' ms'
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
  yAxis: [{
      gridIndex: 0,
      name: 'Throughput [Millions tx/s]',
      // axisLabel: { formatter: '{value} ms'},
    },
    { gridIndex: 1,
      name: 'Throughput [Millions tx/s]',
      // axisLabel: { formatter: '{value} ms'},
  }],
  grid: [
    {
      bottom: '55%',
      tooltip: {
        trigger: 'axis',
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
        // label: { formatter: 'Sequential'},
      },
    },
    {
      name: 'Sequential', data: [], type: 'line',
      markLine: {
        data: [{ yAxis: sequential_latency }],
        // label: { formatter: 'Sequential'},
      },
    },
    ...top_series,
    ...bottom_series,
  ]
};