package ciir.proteus;

import java.io.File
import org.lemurproject.galago.tupleflow.Parameters

object Hestia {
  def argsAsJSON(argv: Array[String]) = {
    val (files, args) = argv.partition { arg => new File(arg).exists() }

    val parameters = new Parameters()
    
    // load all json files given
    files.map(f => {
      parameters.copyFrom(Parameters.parse(new File(f)))
    })

    // read parameters from arguments next
    parameters.copyFrom(new Parameters(args));

    parameters
  }
}


import gnu.trove.map.hash._
import gnu.trove.set.hash._
import ciir.proteus.galago.{Handler, Searchable, WordHistory}
import ciir.proteus.galago.CollectionHandler
import org.lemurproject.galago.core.retrieval.ScoredDocument
import org.lemurproject.galago.core.retrieval.processing.ScoringContext
import org.lemurproject.galago.core.retrieval.Retrieval
import org.lemurproject.galago.core.retrieval.LocalRetrieval

//  metadata cache - super important to making this run fast
//  if metadata not in kernel 34 seconds to look up 1000 docs
//  if metadata in kernel, 8 seconds to look up 1000 docs
//  if metadata in this hash, 1 ms to look up 1000 docs :)
class DateCache(val handler: Handler with Searchable) {
  var retrieval = handler.retrieval.asInstanceOf[LocalRetrieval]
  var index = retrieval.getIndex
  
  // updated by init
  private var docDates = new TIntIntHashMap()
  private var dateToWordCount = new TIntIntHashMap()
  private var dateToBookCount = new TIntIntHashMap()
  var minDate = 4096
  var maxDate = -1

  init()

  private def initDocDate(id: Int) = {
    var date = -1
    if(!docDates.containsKey(id)) {
      try {
        val name = retrieval.getDocumentName(id)
        date = handler.asInstanceOf[CollectionHandler].readDocumentDate(name)
        docDates.put(id, date)
      } catch {
        // Unknown Document Number exception...
        case e: java.io.IOException => { docDates.put(id, -1) }
      }
    }
    date
  }

  private def init() {
    var lenIter = index.getLengthsIterator()

    val context = new ScoringContext
    lenIter.setContext(context)

    while(!lenIter.isDone) {
      val id = lenIter.currentCandidate
      
      context.document = id
      lenIter.syncTo(id)

      val len = lenIter.getCurrentLength

      if(len > 0) { 
        val date = initDocDate(id)

        if(date != -1 && date < minDate) { minDate = date }
        if(date > maxDate) { maxDate = date }

        dateToWordCount.adjustOrPutValue(date, len, len)
        dateToBookCount.adjustOrPutValue(date, 1, 1)
      }
      lenIter.movePast(id)
    }
  }

  def dateForDoc(id: Int) = docDates.get(id)

  def wordCountForDate(date: Int) = dateToWordCount.get(date)
  def bookCountForDate(date: Int) = dateToBookCount.get(date)

  def inspectDateInfo() {
    var minWords, maxWords, minBooks, maxBooks = 0

    for(date <- dateToWordCount.keys()) {
      val words = dateToWordCount.get(date)
      val books = dateToBookCount.get(date)

      if(books < minBooks) { minBooks = books }
      if(books > maxBooks) { maxBooks = books }
      if(words < minWords) { minWords = words }
      if(words > maxWords) { maxWords = words }
    }

    println("Index:")
    println("  numDates: " + dateToWordCount.size)
    printf("  years: [%d,%d]\n", minDate, maxDate)
    printf("  minBooks: %d, maxBooks: %d\n", minBooks, maxBooks)
    printf("  minWords: %d, maxWords: %d\n", minWords, maxWords)
  }

  def domain = minDate to maxDate

  def size = docDates.size
}

object TextFile {
  import java.io._

  def write(fileName: String, action: PrintWriter=>Unit) {
    val fp = new PrintWriter(fileName)
    try { action(fp) } finally { fp.close() }
  }
}

object CurveDataBuilder {
  var dateCache: DateCache = null

  def queryToWordCurve(retrieval: Retrieval, query: String) = {
    var weights = new TIntDoubleHashMap

    // run query
    val (_, sdocs) = WordHistory.runQuery(retrieval, query)

    // date all returned documents
    sdocs.foreach(sdoc => {
      val date = dateCache.dateForDoc(sdoc.document)
      val score = sdoc.score
      weights.adjustOrPutValue(date, score, score)
    })

    // return normalized curve
    for(date <- dateCache.domain) yield {
      if(weights.containsKey(date)) {
        weights.get(date) / dateCache.wordCountForDate(date)
      } else {
        0.0
      }
    }
  }

  def main(args: Array[String]) {
    val parameters = Hestia.argsAsJSON(args)

    if(parameters.getString("siteId").isEmpty) {
      println("Bad configuration file?")
      sys.exit(-1)
    }

    val handlerParms = parameters.getMap("handlers").getMap("collection")
    handlerParms.set("siteId", parameters.getString("siteId"))
    val handler = Handler(ProteusType.valueOf("collection").get, handlerParms).get.asInstanceOf[Handler with Searchable]
    val retrieval = handler.retrieval

    // create date lookup tool
    val tStart = System.currentTimeMillis
    dateCache = new DateCache(handler)
    val tEnd = System.currentTimeMillis
    println("Cached " + dateCache.size + " dates in " + (tEnd-tStart) + "ms!")

    val queries = Seq("lincoln", "basie", "shakespeare", "hamlet")
    val curves = queries.map(queryToWordCurve(retrieval, _))

    TextFile.write("output.csv", out => {
      out.print("date")
      for(d <- dateCache.domain) {
        out.print(", "+ d)
      }
      out.println()

      for(i <- 0 until curves.size) {
        out.print(queries(i))
        for(cd <- curves(i)) {
          out.print(", "+ cd)
        }
        out.println()
      }
    })

  }
}

