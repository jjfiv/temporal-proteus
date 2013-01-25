// make this a global variable
var wordHistoryChart;
var yearHistoryChart;

var haveResultData = function() {
  return typeof(resultData) !== 'undefined';
};

// this is called when points are clicked on the main graph
var displayPieChart = function(seriesName, year) {
  if(!haveResultData()) {
    return;
  }

  var books = resultData.filter(function (s) { return s.name === seriesName; });
  // make sure there's a series to display
  if( books.length !== 1 ) {
    console.log("no series to display");
    console.log(resultData);
    return;
  }

  var relevant_books = books[0].raw.filter(function(bk) { return bk.year === year; });


  var hits = relevant_books.map(function(bk) { return {name: bk.id, y: bk.weight}; });

  // clear old chart ?
  if(yearHistoryChart) { yearHistoryChart.destroy(); yearHistoryChart = undefined; }

  yearHistoryChart = new Highcharts.Chart({
    chart: {
      renderTo: "yearFreqs",
      type: "pie"
    },
    title: {
      text: 'Term Frequency for "' + seriesName + '" in ' + year
    },
    plotOptions: {
      series: {
        cursor: 'pointer',
        events: {
          click: function(event) {
            var bookId = event.point.name;
            // go to the lookup? page for the selected book
            window.location.href = "details?id="+encodeURIComponent(bookId)+"--sCollection--sportland";
          }
        }
      }
    },
    series: [{
      type: 'pie',
      data: hits
    }]
  });

};

// this is called on load, to create the main graph
$(document).ready(function() {
  // early exit if no data has been returned
  if(!haveResultData())
    return;
  
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
    plotOptions: {
      series: {
        cursor: 'pointer',
        events: {
          click: function(event) {
            var series = this.name;
            var year = event.point.x;

            displayPieChart(series, year);
          }
        }
      }
    },
    series: graphData
  });
});

