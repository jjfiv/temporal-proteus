package ciir.proteus

import org.lemurproject.galago.core.index.Index
import org.lemurproject.galago.core.index.ValueIterator
import org.lemurproject.galago.core.retrieval.iterator.MovableCountIterator
import org.lemurproject.galago.core.retrieval.ScoredDocument
import org.lemurproject.galago.core.retrieval.processing.ScoringContext
import org.lemurproject.galago.tupleflow.Parameters

object Util {
  import java.io._


  def argsAsJSON(argv: Array[String]): Parameters = {
    val (files, args) = argv.partition { arg => new File(arg).exists() }

    var parameters = new Parameters()
    
    // load all json files given
    files.map(f => { parameters.copyFrom(Parameters.parse(new File(f))) })

    // read parameters from arguments next
    parameters.copyFrom(new Parameters(args));

    parameters
  }

  def fileExists(fileName: String) = {
    new File(fileName).exists()
  }

  def printToFile(fileName: String, op: PrintWriter=>Unit) {
    var p = new PrintWriter(new File(fileName))
    try { op(p) } finally { p.close() }
  }

  def firstOfPair[A,B](tuple: Tuple2[A,B]) = tuple match {
    case Tuple2(x, _) => x
  }
  def secondOfPair[A,B](tuple: Tuple2[A,B]) = tuple match {
    case Tuple2(_, y) => y
  }
  def timed[A](desc: String, block: =>A): A = {
    val ti = System.currentTimeMillis
    val result = block
    val tf = System.currentTimeMillis
    println("timed: \""+desc+"\" took "+(tf-ti)+"ms!")
    result
  }
  def fraction(a: Int, b: Int): Double = {
    if(b == 0) {
      0.0
    } else {
      a.toDouble / b.toDouble
    }
  }
  def harmonicMean(a: Double, b: Double) = { (2*a*b) / (a+b) }
  def loopUntil(max: Int)(op: Int=>Unit) {
    var i=0
    while(i < max) {
      op(i)
      i+=1
    }
  }
}

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
  
  def forDocInInvertedList(iter: MovableCountIterator, block: (Int,Int)=>Unit) {
    while(!iter.isDone) {
      val doc = iter.currentCandidate
      iter.getContext.document = doc
      val count = iter.count()
      block(doc, count)
      iter.movePast(doc)
    }
  }

  def forDocInIndex(index: Index, block: Int=>Unit) {
    var lenIter = index.getLengthsIterator

    while(!lenIter.isDone) {
      val id = lenIter.currentCandidate()
      block(id)
      lenIter.movePast(id)
    }
  }


  def forDocLenInIndex(index: Index, block: (Int,Int)=>Unit) {
    var lenIter = index.getLengthsIterator
    var context = new ScoringContext
    lenIter.setContext(context)

    while(!lenIter.isDone) {
      val id = lenIter.currentCandidate
      context.document = id
      lenIter.syncTo(id)

      block(id, lenIter.getCurrentLength)
      lenIter.movePast(id)
    }
  }
}


