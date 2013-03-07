package ciir.proteus;

import java.io.File
import org.lemurproject.galago.tupleflow.Parameters
import ciir.proteus.galago.DateCache
import gnu.trove.map.hash._
import ciir.proteus.galago.{Handler, Searchable, WordHistory}
import ciir.proteus.galago.CollectionHandler
import org.lemurproject.galago.core.retrieval.{Retrieval, LocalRetrieval}

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

import org.lemurproject.galago.core.index.Index
import org.lemurproject.galago.core.retrieval.ScoredDocument
class Vocabulary(retrieval: Retrieval, index: Index) {
  def init(index: Index) = {
    val indexPartReader = index.getIndexPart("postings.porter")
    if(indexPartReader == null) {
      sys.exit(-1)
    }

    var keyIter = indexPartReader.getIterator
    var keyBuilder = Vector.newBuilder[String]
    var total = 0
    var kept = 0

    while(!keyIter.isDone) {
      val str = keyIter.getKeyString

      total += 1

      val nonLetters = str.exists(!_.isLetter)

      if(total % 10000 == 0) { println(total) }

      if(!nonLetters && total % 50 == 0) {
        val (_, sdocs: Array[ScoredDocument]) = WordHistory.runQuery(retrieval, str)
        if(sdocs.length >= 2) {
          kept += 1
          keyBuilder += str
        }
      }

      keyIter.nextKey
    }

    printf("Evaluated %d query terms, kept %d\n", total, kept)

    keyBuilder.result
  }

  val data = init(index)
}

object CurveDataBuilder {
  var dateCache: DateCache = null

  def queryToWordCurve(retrieval: Retrieval, query: String): IndexedSeq[Double] = {
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

  def diffCurve(a: IndexedSeq[Double], b: IndexedSeq[Double]) = {
    def abs(x: Double) = if (x < 0) { -x } else { x }
    a.zip(b).map( _ match { case Tuple2(x,y) => abs(x - y) } ).sum
  }

  def main(args: Array[String]) {
    val parameters = Hestia.argsAsJSON(args)

    if(parameters.getString("siteId").isEmpty) {
      println("Bad configuration file?")
      sys.exit(-1)
    }

    val handlerType = "collection"
    val handlerParms = parameters.getMap("handlers").getMap(handlerType)
    handlerParms.set("siteId", parameters.getString("siteId"))
    val handler = Handler(ProteusType.valueOf(handlerType).get, handlerParms).get.asInstanceOf[Handler with Searchable]
    val retrieval = handler.retrieval.asInstanceOf[LocalRetrieval]
    val index = retrieval.getIndex
    
    val vocab = new Vocabulary(retrieval, index)

    // create date lookup tool
    dateCache = handler.asInstanceOf[CollectionHandler].dateCache

    val queryTerm = "lincoln"
    val queryCurve = queryToWordCurve(retrieval, queryTerm)


    var i=0
    val scores = for( term <- vocab.data ) yield {
      val q = new String(term)
      val curve = queryToWordCurve(retrieval, q)
      if(i % 10000 == 0) { println(i) }
      i+=1
      //diff curves..
      diffCurve(queryCurve, curve)
    }

    val scored_terms = scores.zip(vocab.data).sortBy( _ match {
      case Tuple2(x, y) => x
    }).take(100)

    scored_terms.map(println)
  }
}

