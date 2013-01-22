
// make this a global variable
var wordHistoryChart;

$(document).ready(function() {
  // early exit if no data has been returned
  if(typeof(resultData) === 'undefined')
    return;
  
  console.log(resultData);
  var graphData = [];
  
  resultData.forEach(function(query) {
    var cur = {name: query.name, data: []};
    var curYear = -1;

    query.raw.forEach(function(book) {
      if(book.year != curYear) {
        cur.data.push([book.year, book.weight]);
        curYear = book.year;
      } else {
        // increase the weight of the last element
        cur.data[cur.data.length-1][1] += book.weight;
      }
    });
    
    graphData.push(cur);
  });

  console.log(graphData);

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
      // by having a custom formatter, prevent thousands commas
      labels: { formatter: function() { return this.value; } },
      allowDecimals: false
    },
    yAxis: {
      allowDecimals: false
    },
    series: graphData
  });
});

