package ciir.proteus;

import collection.mutable.ArrayBuffer
import java.io.File
import gnu.trove.map.hash._

import ciir.proteus.galago.DateCache
import ciir.proteus.galago.{Handler, Searchable, WordHistory}
import ciir.proteus.galago.CollectionHandler

import org.lemurproject.galago.core.index.Index
import org.lemurproject.galago.tupleflow.Parameters
import org.lemurproject.galago.core.retrieval.{Retrieval, LocalRetrieval}
import org.lemurproject.galago.tupleflow.StreamCreator

class Vocabulary(var dateCache: DateCache, val fileStore: String, var retrieval: Retrieval, var index: Index) {
  // for identifiying files
  val MagicNumber = 0xf0cabe14

  // updated by init or load
  var terms = Array[String]()
  var freqData = Array[TimeCurve]()
  
  val t0 = System.currentTimeMillis
  if(Util.fileExists(fileStore)) {
    loadFromFile(fileStore)
  } else {
    init()
  }
  val tf = System.currentTimeMillis
  println("Init Vocabulary in "+(tf-t0)+"ms!")
  
  def init() = {
    var total = 0
    var keyBuilder = new ArrayBuffer[String]()
    var curveBuilder = new ArrayBuffer[TimeCurve]()

    GalagoIndexUtil.forKeyInIndex(index, "postings.porter", (key, valueIter) => {
      total += 1

      if(total % 10000 == 0) { println("read postings "+total) }

      var docsForKey = new ArrayBuffer[Int]()
      var countsForKey = new ArrayBuffer[Int]()
      
      GalagoIndexUtil.forDocInInvertedList(valueIter, (doc, count) => {
        docsForKey += doc
        countsForKey += count
      })

      var numDocs = docsForKey.size

      if(numDocs >= 2) {
        keyBuilder += key

        var results = new TIntIntHashMap

        var i = 0
        while(i < numDocs) {
          val date = dateCache.dateForDoc(docsForKey(i))
          if(date > 0) {
            results.adjustOrPutValue(date, countsForKey(i), countsForKey(i))
          }
          
          i+=1
        }

        curveBuilder += TimeCurve.ofTroveMap(dateCache, key, results)
      }
    })

    println("Kept: " + keyBuilder.result.size + ", Total: " + total)

    terms = keyBuilder.result.toArray
    freqData = curveBuilder.result.toArray

    if(fileStore.length != 0) {
      saveToFile(fileStore)
    }
  }

  def loadFromFile(fileName: String) {
    var dis = StreamCreator.openInputStream(fileName)

    var error = true

    try {
      val magicNum = dis.readInt
      if(magicNum != MagicNumber) {
        printf("Tried to interpret file \"%s\" as a Vocabulary object, bad MagicNumber 0x%x != 0x%x!\n", fileName, magicNum, MagicNumber )
        throw new Error
      }

      val count = dis.readInt
      
      var keyBuilder = new ArrayBuffer[String]()
      var curveBuilder = new ArrayBuffer[TimeCurve]()

      var i=0
      while(i < count) {
        //if(i % 10000 == 0) { println("load vocab "+i); }

        val term = dis.readUTF
        val curve = TimeCurve.unencode(dis, term, dateCache)

        if(curve.data.sum > 0) {
          keyBuilder += term
          curveBuilder += curve
        }

        i+=1
      }

      terms = keyBuilder.result.toArray
      freqData = curveBuilder.result.toArray

      error = false
    } catch {
      case err: Error => { }
    } finally {
      dis.close()
      
      // if we failed to load, init from index & corpus metadata
      if(error) { init() }
    }
  }

  def saveToFile(fileName: String) {
    var dos = StreamCreator.openOutputStream(fileName)

    try {
      dos.writeInt(MagicNumber)
      dos.writeInt(terms.size)

      var i=0
      while(i < terms.size) {
        if(i % 10000 == 0) { println("writeVocab "+i) }

        dos.writeUTF(terms(i))
        TimeCurve.encode(dos, freqData(i))

        i+=1
      }

    } finally {
      dos.close()
    }
  }
}

trait CurveMaker {
  def search(query: String): TimeCurve
  def domain: Range
  def maxForYear(year: Int): Int
  def fromData(name: String, data: Array[Int]): TimeCurve

  // defined in terms of above primitives
  def maxArray = domain.map(maxForYear).toArray
  def maxCurve = fromData("__MAX__", maxArray)
}

case class GalagoCurveMaker(retrieval: Retrieval, dateCache: DateCache) extends CurveMaker {
  def domain = dateCache.domain
  def search(query: String): TimeCurve = {
    var results = new TIntIntHashMap

    // run query
    val (_, sdocs) = WordHistory.runQuery(retrieval, query)

    // date all returned documents
    sdocs.foreach(sdoc => {
      val date = dateCache.dateForDoc(sdoc.document)
      if(date > 0) {
        val score = sdoc.score.toInt
        results.adjustOrPutValue(date, score, score)
      }
    })
    
    TimeCurve.ofTroveMap(dateCache, query, results)
  }
  def maxForYear(year: Int) = dateCache.wordCountForDate(year)
  def fromData(name: String, data: Array[Int]) = new TimeCurve(dateCache, name, data)
}

object CurveDataBuilder {
  var dateCache: DateCache = null

  def main(args: Array[String]) {
    val parameters = Util.argsAsJSON(args)

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
    
    // grab date lookup tool
    dateCache = handler.asInstanceOf[CollectionHandler].dateCache

    val curveMaker = GalagoCurveMaker(retrieval, dateCache)

    // create dated vocabulary
    val vocab = Util.timed("Creating or Loading Vocabulary", {
      new Vocabulary(dateCache, handlerParms.getString("curveCache"), retrieval, index)
    })

    val originalVocab = vocab.freqData

    val bfSearch = new BruteForceSearch(curveMaker, originalVocab)
    val lshSearch = new LSHSearch(curveMaker, originalVocab)

    val query = "franklin"
    val assumedRelevant = 100
    val numRelevant = 100
    val numPossible = originalVocab.size

    // use the brute-force search as our ground truth to select the top 100 terms and call these relevant
    val relevantTerms = Util.timed("brute-force search; gen relevant", {
      bfSearch.run(query, assumedRelevant).map(_.term).toSet
    })
    assert(relevantTerms.contains(query))


    // now evaluate the search method in currentSearch
    //val currentSearch: CurveSearch = binLSHSearch

    def evaluate(currentSearch: CurveSearch) {
      println("")
      println("Evaluate: "+currentSearch.name)
      println("==")

      val rankedList = Util.timed("search runtime", {
        currentSearch.run(query, numPossible).map(_.term)
      })
      
      val retrievedTerms = rankedList.take(numRelevant).toSet
      // take as many as necessary to find all relevant documents
      //var relCount = 0
      //var retCount = 0
      //while(relCount < numRelevant && retCount < rankedList.size) {
      //  if(relevantTerms.contains(rankedList(retCount))) {
      //    relCount += 1
      //  }
      //  retCount += 1
      //}
      //val retrievedTerms = rankedList.take(retCount).toSet

      
      val rrTerms = relevantTerms.intersect(retrievedTerms)
      
      printf("Terms Retrieved: %d, Terms Relevant: %d\n", retrievedTerms.size, rrTerms.size)
      
      val precision = Util.fraction(rrTerms.size, retrievedTerms.size)
      val recall = Util.fraction(rrTerms.size, numRelevant)
      val fmeasure = Util.harmonicMean(precision, recall)

      printf("Precision: %1.4f\n", precision)
      printf("Recall: %1.4f\n", recall)
      printf("F1: %1.4f\n", fmeasure)
      
      val topRank = rankedList.indexOf(query)+1
      printf("Rank of Top Hit: %d ; MRR = %1.4f\n",
        topRank, Util.fraction(1, topRank))
    }

    evaluate(bfSearch)
    evaluate(lshSearch)
  }
}


