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

object CurveDataBuilder {
  import gnu.trove.map.hash._

  //  metadata cache - super important to making this run fast
  //  if metadata not in kernel 34 seconds to look up 1000 docs
  //  if metadata in kernel, 8 seconds to look up 1000 docs
  //  if metadata in this hash, 1 ms to look up 1000 docs :)
  val docDates = new TIntIntHashMap()

  def lookupDates(handler: Handler, sdocs: Array[ScoredDocument]) = {
    val t0 = System.currentTimeMillis
    val results = sdocs.map(sdoc => {
      val name = sdoc.documentName
      val id = sdoc.document
      val score = sdoc.score.toInt

      if(!docDates.containsKey(id)) {
        docDates.put(id, handler.asInstanceOf[CollectionHandler].readDocumentDate(name))
      }
      
      (name, id, score, docDates.get(id))
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
    val handler = Handler(ProteusType.valueOf("collection").get, handlerParms).get
    val retrieval = handler.asInstanceOf[Handler with Searchable].retrieval

    val (_, results) = WordHistory.runQuery(retrieval, "lincoln")
    lookupDates(handler, results)
    lookupDates(handler, results)
    lookupDates(handler, results)



  }
}

