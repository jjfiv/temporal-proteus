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

import ciir.proteus.galago.{Handler, Searchable, WordHistory}
import ciir.proteus.galago.CollectionHandler
import org.lemurproject.galago.core.retrieval.ScoredDocument
import org.lemurproject.galago.core.retrieval.Retrieval

//  metadata cache - super important to making this run fast
//  if metadata not in kernel 34 seconds to look up 1000 docs
//  if metadata in kernel, 8 seconds to look up 1000 docs
//  if metadata in this hash, 1 ms to look up 1000 docs :)
class DateCache(val handler: Handler with Searchable) {
  val retrieval = handler.retrieval
  val docDates = new gnu.trove.map.hash.TIntIntHashMap()

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

    println(retrieval.getGlobalParameters())

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



  }
}

