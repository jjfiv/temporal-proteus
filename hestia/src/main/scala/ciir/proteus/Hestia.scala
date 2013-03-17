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
import org.lemurproject.galago.core.retrieval.ScoredDocument
class Vocabulary(val fileStore: String, var retrieval: Retrieval, var index: Index) {
  
  // for identifiying files
  val MagicNumber = 0x70cabe14

  // updated by init or load
  var data = Vector[String]()
  
  val t0 = System.currentTimeMillis
  if(Util.fileExists(fileStore)) {
    loadFromFile(fileStore)
  } else {
    init()
  }
  val tf = System.currentTimeMillis
  println("Init Vocabulary in "+(tf-t0)+"ms!")
  
  def init() = {
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
  }

  def loadFromFile(fileName: String) {
    var fis = new java.io.FileInputStream(fileName)
    var dis = new java.io.DataInputStream(fis)

    var error = true

    try {
      val magicNum = dis.readInt
      if(magicNum != MagicNumber) {
        printf("Tried to interpret file \"%s\" as a Vocabulary object, bad Magic Number 0x%x != 0x%x!\n", fileName, magicNum, MagicNumber )
      }

      val count = dis.readInt
      
      var keyBuilder = Vector.newBuilder[String]

      for(i <- 0 until count) {
        val str = dis.readUTF
        if(i % 120 == 0) {
          keyBuilder += str
        }
      }

      data = keyBuilder.result
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
      dos.writeInt(data.size)

      data.foreach(s => {
        dos.writeUTF(s)
      })

    } finally {
      fos.close()
    }
  }
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

  def diffCurve(a: IndexedSeq[Double], b: IndexedSeq[Double]): Double = {
    def sqr(x: Double) = x*x
    a.zip(b).map({ case Tuple2(x,y) => sqr(x - y) } ).sum
  }

  def curveBasedQuery(term: String, retrieval: LocalRetrieval, data: Vector[String]): Array[Double] = {
    val queryTerm = "lincoln"
    val queryCurve = queryToWordCurve(retrieval, queryTerm)
    
    var i=0
    val vlen = data.size
    var scores = new Array[Double](vlen)
    
    while(i < vlen) {
      scores(i) = diffCurve(queryCurve, queryToWordCurve(retrieval, data(i)))
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
    
    val vocab = new Vocabulary(handlerParms.getString("curveVocabCache"), retrieval, index)

    // create date lookup tool
    dateCache = handler.asInstanceOf[CollectionHandler].dateCache



    val start_query = System.currentTimeMillis
    val scores = curveBasedQuery("lincoln", retrieval, vocab.data)
    val end_query = System.currentTimeMillis
    println("Scoring took "+ (end_query-start_query) + "ms!")
    
    val start_sort = System.currentTimeMillis
    val scored_terms = scores.zip(vocab.data).sortBy({
      case Tuple2(x, y) => x
    }).take(20)
    val end_sort = System.currentTimeMillis
    println("Sorting took "+ (end_sort-start_sort) + "ms!")

    scored_terms.map(println)

  }
}

