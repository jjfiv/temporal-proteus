package ciir.proteus

import ciir.proteus.galago.DateCache
import org.lemurproject.galago.tupleflow.Utility
import gnu.trove.map.hash._

object TimeCurve {
  def encode(dos: java.io.DataOutputStream, tc: TimeCurve) {
    //tc.data.foreach(Utility.compressInt(dos, _))
    //tc.data.foreach(dos.writeShort(_))

    val map = tc.data
    dos.writeInt(map.size)

    for(date <- map.keys) {
      val count = map.get(date)

      dos.writeShort(date)
      dos.writeShort(count)
    }
  }

  def unencode(dis: java.io.DataInputStream, numDates: Int): TimeCurve = {
    //var pts = new Array[Int](numDates)
    //var j=0
    //while(j < numDates) {
    //  //pts(j) = dis.readShort
    //  pts(j) = Utility.uncompressInt(dis)
    //  j+=1
    //}
    //new TimeCurve(pts)
    var pts = new TIntIntHashMap
    val size = dis.readInt

    for(i <- 0 until size) {
      val date = dis.readShort
      val count = dis.readShort
      pts.put(date, count)
    }
    new TimeCurve(pts)
  }

  /*
  def compare(dateCache: DateCache, a: TimeCurve, b: TimeCurve): Double = {
    def sqr(x: Double) = x*x

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
  */
}

// sparse representation of time curve more efficient?
class TimeCurve(val data: TIntIntHashMap) {
  def score(against: TimeCurve, dateCache: DateCache): Double = {
    val allDates = data.keys().toSet.union(against.data.keys().toSet)

    var result = 0.0

    for(date <- allDates) {
      val count = dateCache.wordCountForDate(date)
      
      if(count != 0) {
        val maxFreq = count.toDouble

        val a = data.get(date).toDouble / maxFreq
        val b = against.data.get(date).toDouble / maxFreq

        result += (a-b)*(a-b)
      }
    }
    result
  }

}
/*
class TimeCurve(val data: Array[Int]) {
  def size = data.size

  def classifyAgainst(planeNormal: TimeCurve): Boolean = {
    def sign[A](x: Int): Int = { if(x < 0) -1 else 1 }
    // take the difference of each point, and dot product it, so as to classify input points as being either to the left or the right of it, represented as a boolean
    planeNormal.data.zip(data).map({
      case Tuple2(a, b) => sign(a - b)
    }).sum >= 0
  }
}
*/


