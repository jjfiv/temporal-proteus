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

  def main(args: Array[String]) {
    println("This doesn't do anything.");
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
  val retrieval = handler.retrieval
  val docDates = new TIntIntHashMap()
  // map of date -> set[doc id]
  //val datesToDoc = new TIntObjectHashMap[TIntHashSet]()

  def lookupDate(id: Int) = {
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
    } else {
      date = docDates.get(id)
    }
    
    // build reverse-map as well
    /*
    if(!datesToDoc.containsKey(date)) {
      var docSet = new TIntHashSet
      datesToDoc.put(date, docSet)
    }
    datesToDoc.get(date).add(id)
    */

    date
  }

  def fill() {
    val collectionStats = retrieval.getCollectionStatistics("#lengths:document:part=lengths()")
    val numDocs = collectionStats.documentCount.toInt
    (0 to numDocs).map(lookupDate)
  }

  def size = docDates.size
}

object CurveDataBuilder {
  var dateCache: DateCache = null

  def lookupDates(sdocs: Array[ScoredDocument]) = {
    val t0 = System.currentTimeMillis
    val results = sdocs.map(sdoc => {
      val name = sdoc.documentName
      val id = sdoc.document
      val score = sdoc.score.toInt
      val date = dateCache.lookupDate(id)

      (name, id, score, date)
    })
    val t1 = System.currentTimeMillis

    println("Metadata lookup took: " + (t1-t0) + "ms!")

    results
  }

  def inspectIndex(index: org.lemurproject.galago.core.index.Index) {
    val partNames = index.getPartNames()
    var lenIter = index.getLengthsIterator()

    var dateToWordCount = new TIntIntHashMap()
    var dateToBookCount = new TIntIntHashMap()
    var idToLength = new TIntIntHashMap()
    var maxLen = 0
    var minLen = 1000000

    val scoringContext = new ScoringContext
    lenIter.setContext(scoringContext)

    while(!lenIter.isDone) {
      val id = lenIter.currentCandidate()
      
      scoringContext.document = id
      lenIter.syncTo(id)

      val len = lenIter.getCurrentLength
      //println(id, len)

      if(len > 0) { 
        if(len > maxLen) {
          maxLen = len
        }
        if(len < minLen) {
          minLen = len
        }

        val date = dateCache.lookupDate(id)
        dateToWordCount.adjustOrPutValue(date, len, len)
        dateToBookCount.adjustOrPutValue(date, 1, 1)

        idToLength.put(id, len)
      }
      lenIter.movePast(id)
    }

    var minDate = 4096 // I'll be long dead before this constant is bad
    var maxDate = -1 
    // iterate over dates now:
    for(d <- dateToWordCount.keys) {
      if(d != -1) {
        if(d < minDate) { minDate = d }
        if(d > maxDate) { maxDate = d }
      }
    }
    
    println("Index:")
    println("  partNames: " + partNames)
    println("  size: " + idToLength.size)
    println("  maxLen: " + maxLen + " minLen: " + minLen)
    println("  numDates: " + dateToWordCount.size)
    printf("  years: [%d,%d]\n", minDate, maxDate)
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
    dateCache = new DateCache(handler)

    val tStart = System.currentTimeMillis
    dateCache.fill()
    val tEnd = System.currentTimeMillis
    println("Cached " + dateCache.size + " dates in " + (tEnd-tStart) + "ms!")

    def doDateQuery(retrieval: Retrieval, query: String) = {
      val (_, results) = WordHistory.runQuery(retrieval, query)
      lookupDates(results)
    }

    doDateQuery(retrieval, "lincoln")
    doDateQuery(retrieval, "basie")
    doDateQuery(retrieval, "shakespeare")
    
    inspectIndex(retrieval.asInstanceOf[LocalRetrieval].getIndex())

  }
}

