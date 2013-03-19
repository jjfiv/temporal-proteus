package ciir.proteus

import ciir.proteus.galago.DateCache
import org.lemurproject.galago.tupleflow.Utility
import gnu.trove.map.hash._
import gnu.trove.procedure._
import collection.mutable.ArrayBuffer

object TimeCurve {
  var unionTime = 0.0
  var compareTime = 0.0

  def encode(dos: java.io.DataOutputStream, tc: TimeCurve) {
    val arr = tc.data
    var map = new TIntIntHashMap

    val minDate = tc.dateCache.minDate
    var i=0;
    while(i < arr.size) {
      val count = arr(i)
      if(count > 0) {
        val date = minDate + i
        map.put(date, count)
      }
      i+=1
    }
    
    dos.writeInt(map.size)
    for(date <- map.keys) {
      val count = map.get(date)
      dos.writeShort(date)
      dos.writeShort(count)
    }
  }

  def unencode(dis: java.io.DataInputStream, dateCache: DateCache): TimeCurve = {
    var pts = new TIntIntHashMap
    val size = dis.readInt

    for(i <- 0 until size) {
      val date = dis.readShort
      val count = dis.readShort
      pts.put(date, count)
    }
    TimeCurve.ofTroveMap(dateCache, pts)
  }

  def ofTroveMap(dateCache: DateCache, data: TIntIntHashMap) = {
    val numDates = dateCache.numDates
    val minDate = dateCache.minDate
    var arr = new Array[Int](numDates)
    
    data.keys.foreach(date => {
      val count = data.get(date)
      if(count > 0 && date > 0) {
        val index = date - minDate
        if(!(index >= 0 && index <= 140)) {
          println(index)
          println(date)
          println(minDate)
          println(numDates)
          assert(false)
        }
        arr(index) = count
      }
    })

    new TimeCurve(dateCache, arr)
  }
}

// sparse representation of time curve more efficient?
class TimeCurve(val dateCache: DateCache, val data: Array[Int]) {
  
  def score(against: TimeCurve): Double = {
    def sqr(x: Double) = x*x

    val a = data
    val b = against.data
    var score = 0.0

    var i=0
    while(i < a.size) {
      val date = dateCache.minDate + i
      val count = dateCache.wordCountForDate(date)
      
      if(count != 0) {
        val maxFreq = count.toDouble
        val x = a(i).toDouble / maxFreq
        val y = b(i).toDouble / maxFreq

        score += sqr(x - y)
      }
      i+=1
    }

    score
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


