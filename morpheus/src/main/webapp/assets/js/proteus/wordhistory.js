
// make this a global variable
var wordHistoryChart;

$(document).ready(function() {
  // early exit if no data has been returned
  if(typeof(resultData) === 'undefined')
    return;

  //temporary: #{wordHistoryResultsToJS(q, results)}

  wordHistoryChart = new Highcharts.Chart({
    chart: {
      renderTo: 'wordFreqs',
      type: 'line'
    },
    rangeSelector: { enabled : false },
    scrollbar : { enabled: false },
    navigator : { enabled: false },
    legend : { enabled: true },
    title: {
      text: 'Word Use Over Time'
    },
    xAxis: {
      allowDecimals: false
    },
    yAxis: {
      allowDecimals: false
    },
    series: [resultData]
  });
});

