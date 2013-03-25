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

sealed case class ScoredTerm(term: String, score: Double);

class RankedList(val numHits: Int) {
  var results = new Array[ScoredTerm](numHits)
  var count = 0

  def insert(newest: ScoredTerm) {
    if(count < numHits) {
      results(count) = newest
      count += 1
      return
    }

    // discard new things with tiny scores, special case
    if(results.last.score < newest.score) {
      return
    }

    // chop current array in pieces and reassemble
    val (better, worse) = results.partition(res => res.score < newest.score)
    results = (better :+ newest) ++ worse.dropRight(1)
  }
}

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
        if(i % 10000 == 0) { println("load vocab "+i); }

        val term = dis.readUTF
        val curve = TimeCurve.unencode(dis, term, dateCache)

        keyBuilder += term
        curveBuilder += curve

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

object CurveDataBuilder {
  var dateCache: DateCache = null

  def queryToWordCurve(retrieval: Retrieval, query: String): TimeCurve = {
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

    // create dated vocabulary
    val vocab = Util.timed("Creating or Loading Vocabulary", {
      new Vocabulary(dateCache, handlerParms.getString("curveCache"), retrieval, index)
    })

    val originalVocab = vocab.freqData
    // TODO, LSH
    
    var randomizer = new util.Random(13)
    val randomPlane = new TimeCurve(dateCache, "hash0", dateCache.domain.map(x => randomizer.nextInt).toArray)
    
    val (vocabA, vocabB) = Util.timed("LSH Partitioning", {
      originalVocab.partition(_.classify(randomPlane))
    })

    def jfQuery(term: String) {
      val numResults = 50

      // run query
      val scoredCurves = Util.timed("Scoring", {
        val queryCurve = queryToWordCurve(retrieval, term)

        val hash = queryCurve.classify(randomPlane)

        val data = if (hash) { vocabA } else { vocabB }

        val vlen = data.size
        var rl = new RankedList(numResults)

        data.foreach( timeCurve => {
          rl.insert(ScoredTerm(timeCurve.term, queryCurve.score(timeCurve)))
        })

        rl.results
      })

      scoredCurves.map(println)
    }

    jfQuery("lincoln")
    //jfQuery("abraham")

  }
}


