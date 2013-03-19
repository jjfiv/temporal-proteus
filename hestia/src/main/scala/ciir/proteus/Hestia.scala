package ciir.proteus;

import collection.mutable.ArrayBuffer
import java.io.File
import gnu.trove.map.hash._

import ciir.proteus.galago.DateCache
import ciir.proteus.galago.{Handler, Searchable, WordHistory}
import ciir.proteus.galago.CollectionHandler

import org.lemurproject.galago.tupleflow.Utility
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

object TimeCurve {
  def unencode(dis: java.io.DataInputStream, numDates: Int): TimeCurve = {
    var pts = new Array[Int](numDates)
    var j=0
    while(j < numDates) {
      pts(j) = Utility.uncompressInt(dis)
      j+=1
    }
    new TimeCurve(pts)
  }

  def compare(dateCache: DateCache, a: TimeCurve, b: TimeCurve): Double = {
    def sqr(x: Double) = x*x
    def safe_div(x: Int, y: Int): Double = {
      return x.toDouble / y.toDouble
    }

    var score = 0.0

    var i=0
    while(i < a.size) {
      val date = dateCache.minDate + i
      val count = dateCache.wordCountForDate(date)
      
      if(count != 0) {
        val maxFreq = count.toDouble
        val x = a.data(i).toDouble / maxFreq
        val y = b.data(i).toDouble / maxFreq

        score += sqr(x - y)
      }
      i+=1
    }

    score
  }
}

class TimeCurve(val data: Array[Int]) {
  def encode(dos: java.io.DataOutputStream) {
    data.foreach(Utility.compressInt(dos, _))
  }

  def size = data.size

  def classifyAgainst(planeNormal: TimeCurve): Boolean = {
    def sign[A](x: Int): Int = { if(x < 0) -1 else 1 }
    // take the difference of each point, and dot product it, so as to classify input points as being either to the left or the right of it, represented as a boolean
    planeNormal.data.zip(data).map({
      case Tuple2(a, b) => sign(a - b)
    }).sum >= 0
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

      if(total % 10000 == 0) { println(total) }

      var docsForKey = new ArrayBuffer[Int]()
      var countsForKey = new ArrayBuffer[Int]()
      while(!valueIter.isDone) {
        val doc = valueIter.currentCandidate
        docsForKey += doc
        countsForKey += valueIter.count()
        valueIter.movePast(doc)
      }

      var numDocs = docsForKey.size

      if(numDocs >= 2) {
        keyBuilder += key

        val numDates = (dateCache.maxDate - dateCache.minDate) + 1
        var results = Array.fill(numDates) { 0 }

        results
        var i = 0
        while(i < numDocs) {
          val date = dateCache.dateForDoc(docsForKey(i))
          if(date != -1) {
            val index = date - dateCache.minDate
            results(index) += countsForKey(i)
          }
          
          i+=1
        }

        curveBuilder += new TimeCurve(results)
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
    var fis = new java.io.FileInputStream(fileName)
    var dis = new java.io.DataInputStream(fis)

    var error = true

    try {
      val magicNum = dis.readInt
      if(magicNum != MagicNumber) {
        printf("Tried to interpret file \"%s\" as a Vocabulary object, bad MagicNumber 0x%x != 0x%x!\n", fileName, magicNum, MagicNumber )
        throw new Error
      }

      val count = dis.readInt
      val minDate = dis.readInt
      val maxDate = dis.readInt

      if(minDate != dateCache.minDate || maxDate != dateCache.maxDate) {
        println("Dates messed up!")
        sys.exit(-1)
        throw new Error
      }
      val numDates = (maxDate - minDate)+1
      
      var keyBuilder = new ArrayBuffer[String]()
      var curveBuilder = new ArrayBuffer[TimeCurve]()

      var i=0
      while(i < count) {
        val str = dis.readUTF
        keyBuilder += str

        if(i % 10000 == 0) { printf("load: %1.2f%%\n",100.0*(i.toDouble/count.toDouble)); }

        curveBuilder += TimeCurve.unencode(dis, numDates)
        i+=1
      }

      terms = keyBuilder.result.toArray
      freqData = curveBuilder.result.toArray

      error = false
    } catch {
      case err: Error => { }
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

      dos.writeInt(dateCache.minDate)
      dos.writeInt(dateCache.maxDate)

      var i=0
      while(i < terms.size) {
        if(i % 10000 == 0) { println(i); println(freqData(i).size); println(terms(i)) }
        dos.writeUTF(terms(i))
        freqData(i).encode(dos)

        i+=1
      }

    } finally {
      fos.close()
    }
  }
}

object CurveDataBuilder {
  var dateCache: DateCache = null

  def queryToWordCurve(retrieval: Retrieval, query: String): TimeCurve = {
    val numDates = (dateCache.maxDate - dateCache.minDate) + 1
    var results = new Array[Int](numDates)

    // run query
    val (_, sdocs) = WordHistory.runQuery(retrieval, query)

    // date all returned documents
    sdocs.foreach(sdoc => {
      val date = dateCache.dateForDoc(sdoc.document)
      if(date > 0) {
        val score = sdoc.score.toInt
        val index = date - dateCache.minDate
        results(index) += score
      }
    })
    
    new TimeCurve(results)
  }

  def curveBasedQuery(term: String, retrieval: LocalRetrieval, data: Array[TimeCurve]): Array[Double] = {
    val queryCurve = queryToWordCurve(retrieval, term)
    
    var i=0
    val vlen = data.size
    var scores = new Array[Double](vlen)
    
    while(i < vlen) {
      scores(i) = TimeCurve.compare(dateCache, queryCurve, data(i))
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
    
    // grab date lookup tool
    dateCache = handler.asInstanceOf[CollectionHandler].dateCache

    // create dated vocabulary
    val vocab = Util.timed("Creating or Loading Vocabulary", {
      new Vocabulary(dateCache, handlerParms.getString("curveCache"), retrieval, index)
    })

    // TODO, LSH
    /*
      var randomizer = new util.Random(13)
      val randomPlane = dateCache.domain.map(x => randomizer.nextInt)
      
      val start_partition = System.currentTimeMillis
      val (vocabA, vocabB) = vocab.terms.partition(_.classifyAgainst(randomPlane))
      val end_partition = System.currentTimeMillis
      println("Partitioning took " + (end_partition-start_partition) + "ms!")
    */

    // run query
    val scores = Util.timed("Scoring", {
      curveBasedQuery("lincoln", retrieval, vocab.freqData)
    })
    
    val numResults = 20
    // sort and trim to numResults
    val scored_terms = Util.timed("Sorting", {
      scores.zip(vocab.terms).sortBy(Util.firstOfPair).take(numResults)
    })

    scored_terms.map(println)
  }
}

