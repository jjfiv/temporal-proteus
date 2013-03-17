package ciir.proteus;

import collection.mutable.ArrayBuffer
import java.io.File
import gnu.trove.map.hash._

import ciir.proteus.galago.DateCache
import ciir.proteus.galago.{Handler, Searchable, WordHistory}
import ciir.proteus.galago.CollectionHandler

import org.lemurproject.galago.tupleflow.Parameters
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

object NiceIO {
  import java.io._

  def write(fileName: String, action: PrintWriter=>Unit) {
    var fp = new PrintWriter(fileName)
    try { action(fp) } finally { fp.close() }
  }

  def writeBinary(fileName: String, action: DataOutputStream=>Unit) {
    var fp = new FileOutputStream(fileName)
    var dataOutputStream = new DataOutputStream(fp)
    try { action(dataOutputStream) } finally { fp.close() }
  }
}

import org.lemurproject.galago.core.index.Index
import org.lemurproject.galago.core.index.ValueIterator
import org.lemurproject.galago.core.retrieval.iterator.MovableCountIterator
import org.lemurproject.galago.core.retrieval.ScoredDocument
import org.lemurproject.galago.core.retrieval.processing.ScoringContext

object GalagoIndexUtil {
  def forKeyInIndex(index: Index, indexPartName: String, block: (String,MovableCountIterator)=>Unit) {
    var indexPartReader = index.getIndexPart(indexPartName)
    if(indexPartReader == null) { return }

    var keyIter = indexPartReader.getIterator

    while(!keyIter.isDone) {
      val str = keyIter.getKeyString
      var valueIter = keyIter.getValueIterator.asInstanceOf[MovableCountIterator]
      valueIter.setContext(new ScoringContext)
      block(str, valueIter)
      keyIter.nextKey
    }
  }
}


class Vocabulary(var dateCache: DateCache, val fileStore: String, var retrieval: Retrieval, var index: Index) {
  
  // for identifiying files
  val MagicNumber = 0xf0cabe14

  // updated by init or load
  var terms = Array[String]()
  var freqData = Array[Array[Int]]()
  
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
    var curveBuilder = new ArrayBuffer[Array[Int]]()

    GalagoIndexUtil.forKeyInIndex(index, "postings.porter", (key, valueIter) => {
      total += 1

      if(total % 10000 == 0) { println(total) }

      var docsForKey = new ArrayBuffer[Int]()
      var countsForKey = new ArrayBuffer[Int]()
      while(!valueIter.isDone) {
        val doc = valueIter.currentCandidate
        docsForKey += doc
        countsForKey += valueIter.count()
        valueIter.movePast(doc)
      }

      if(docsForKey.size >= 2) {
        keyBuilder += key
        docsForKey.map(dateCache.dateForDoc(_))
      }
      
    })

    println("Kept: " + keyBuilder.result.size + ", Total: " + total)

    sys.exit(0)
    /*
    while(!keyIter.isDone) {
      val str = keyIter.getKeyString
      
      total += 1
      val nonLetters = str.exists(!_.isLetter)
      
      var documents = Vector.newBuilder[Int]
      var valueIterator = keyIter.getValueIterator
      valueIterator.setContext(new ScoringContext)

      while(!valueIterator.isDone) {
        val document = valueIterator.currentCandidate
        documents += document
        valueIterator.movePast(document)
      }

      if(documents.



      if(total % 10000 == 0) { println(total) }

      if(!nonLetters) {
        val (_, sdocs: Array[ScoredDocument]) = WordHistory.runQuery(retrieval, str)
        if(sdocs.length >= 2) {
          kept += 1
          keyBuilder += str
        }
      }

      keyIter.nextKey
    }

    printf("Evaluated %d query terms, kept %d\n", total, kept)

    data = keyBuilder.result

    if(fileStore.length != 0) {
      saveToFile(fileStore)
    }
    */
  }

  def loadFromFile(fileName: String) {
    var fis = new java.io.FileInputStream(fileName)
    var dis = new java.io.DataInputStream(fis)

    var error = true

    try {
      val magicNum = dis.readInt
      if(magicNum != MagicNumber) {
        printf("Tried to interpret file \"%s\" as a Vocabulary object, bad Magic Number 0x%x != 0x%x!\n", fileName, magicNum, MagicNumber )
        throw new Error
      }

      val count = dis.readInt
      
      var keyBuilder = Vector.newBuilder[String]

      for(i <- 0 until count) {
        val str = dis.readUTF
        if(i % 10 == 0) {
          keyBuilder += str
        }
      }

      terms = keyBuilder.result.toArray
      error = false
    } finally {
      fis.close()
      
      // if we failed to load, init from index & corpus metadata
      if(error) { init() }
    }
  }

  def saveToFile(fileName: String) {
    var fos = new java.io.FileOutputStream(fileName)
    var dos = new java.io.DataOutputStream(fos)

    try {
      dos.writeInt(MagicNumber)
      dos.writeInt(terms.size)

      terms.foreach(s => {
        dos.writeUTF(s)
      })

    } finally {
      fos.close()
    }
  }
}

object CurveDataBuilder {
  var dateCache: DateCache = null

  def scoredDocsToWordCurve(sdocs: Array[ScoredDocument]) {
    
  }

  def queryToWordCurve(retrieval: Retrieval, query: String): Array[Double] = {
    val numDates = (dateCache.maxDate - dateCache.minDate) + 1
    var results = Array.fill(numDates) { 0.0 }

    // run query
    val (_, sdocs) = WordHistory.runQuery(retrieval, query)

    // date all returned documents
    sdocs.foreach(sdoc => {
      val date = dateCache.dateForDoc(sdoc.document)
      if(date > 0) {
        val score = sdoc.score
        val index = date - dateCache.minDate
        results(index) += score
      }
    })

    var i=0
    while(i < numDates) {
      if(results(i) != 0) {
        val date = i+dateCache.minDate
        val dateTF = dateCache.wordCountForDate(date)
        if(dateTF != 0) {
          results(i) /= dateTF
        }
      }

      i+= 1
    }
    results
  }

  def diffCurve(a: Array[Double], b: Array[Double]): Double = {
    def sqr(x: Double) = x*x
    a.zip(b).map({ case Tuple2(x,y) => sqr(x - y) } ).sum
  }

  def classifyCurve(planeNormal: Array[Double], dataPoint: Array[Double]): Boolean = {
    def sign[A](x: Double): Int = { if(x < 0) -1 else 1 }
    // take the difference of each point, and dot product it, so as to classify input points as being either to the left or the right of it, represented as a boolean
    planeNormal.zip(dataPoint).map({
      case Tuple2(norm, data) => sign(norm - data)
    }).sum >= 0
  }

  def curveBasedQuery(term: String, retrieval: LocalRetrieval, data: Array[Array[Double]]): Array[Double] = {
    val queryCurve = queryToWordCurve(retrieval, term)
    
    var i=0
    val vlen = data.size
    var scores = new Array[Double](vlen)
    
    while(i < vlen) {
      scores(i) = diffCurve(queryCurve, data(i))
      i+=1
    }

    scores
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
    
    // create date lookup tool
    dateCache = handler.asInstanceOf[CollectionHandler].dateCache

    // create dated vocabulary
    val vocab = new Vocabulary(dateCache, handlerParms.getString("curveVocabCache"), retrieval, index)


    val vocabCurves: Array[Array[Double]] = Util.timed("Making Curves for Vocabulary", {
      vocab.terms.map(term => queryToWordCurve(retrieval, term))
    })

    var randomizer = new util.Random(13)
    val randomPlane = dateCache.domain.map(x => randomizer.nextDouble)

    // TODO, LSH
    //val start_partition = System.currentTimeMillis
    //val (vocabA, vocabB) = vocab.terms.partition(classifyCurve(randomPlane, _))
    //val end_partition = System.currentTimeMillis
    //println("Partitioning took " + (end_partition-start_partition) + "ms!")

    // run query
    val scores = Util.timed("Scoring", {
      curveBasedQuery("lincoln", retrieval, vocabCurves)
    })
    
    val numResults = 20
    // sort and trim to numResults
    val scored_terms = Util.timed("Sorting", {
      scores.zip(vocab.terms).sortBy(Util.firstOfPair).take(numResults)
    })

    scored_terms.map(println)
  }
}

