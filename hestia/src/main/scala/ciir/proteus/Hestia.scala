package ciir.proteus;

import java.io.File
import org.lemurproject.galago.tupleflow.Parameters
import ciir.proteus.galago.DateCache
import gnu.trove.map.hash._
import ciir.proteus.galago.{Handler, Searchable, WordHistory}
import org.lemurproject.galago.core.retrieval.Retrieval

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

