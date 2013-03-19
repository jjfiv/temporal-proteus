package ciir.proteus

import ciir.proteus.galago.DateCache
import org.lemurproject.galago.tupleflow.Utility

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

