package ciir.proteus

sealed case class ScoredTerm(term: String, score: Double);

trait CurveSearch {
  def run(query: String, numResults: Int): Array[ScoredTerm]
  def name: String
}

class RankedList(val numHits: Int) {
  private var results = new Array[ScoredTerm](numHits)
  private var count = 0

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

  def done = {
    if (count <= numHits) {
      results.take(count).sortBy(_.score)
    } else {
      results
    }
  }
}

class BruteForceSearch(val curveMaker: CurveMaker, val corpus: Array[TimeCurve]) extends CurveSearch {
  def run(query: String, numResults: Int) = {
    val queryCurve = curveMaker.search(query)
    var rl = new RankedList(numResults)

    corpus.foreach( timeCurve => {
      rl.insert(ScoredTerm(timeCurve.term, queryCurve.score(timeCurve)))
    })

    rl.done
  }
  def name = "Brute Force Search"
}

class LSHSearch(val curveMaker: CurveMaker, val corpus: Array[TimeCurve]) extends CurveSearch {
  val numBits = 4
  val numBucketSets = 2
  val numBuckets = (1 << numBits)
  var randomizer = new util.Random(14)

  def allBucketSets = 0 until numBucketSets 
  def allBucketBits = 0 until numBits
  def allBuckets = 0 until numBuckets
  
  def boolToSign(b: Boolean): Int = if (b) -1 else 1
  val hashPlanes: IndexedSeq[IndexedSeq[Array[Int]]] = allBucketSets.map(bktSet => {
    allBucketBits.map(bkt => {
      // generate random boolean vectors of the correct size
      curveMaker.domain.map(x=>boolToSign(randomizer.nextBoolean)).toArray
    })
  })

  val hashBuckets: Array[Array[Set[Int]]] = Util.timed("Bucket whole corpus", { buildBucketsFromCorpus() })
  
  private def buildBucketsFromCorpus() = {
    // make numBucketSets * (2^numBits) bucket builders...
    var bucketBuilders = allBucketSets.map(bktsetno => {
      allBuckets.map(bktno => Array.newBuilder[Int])
    })

    corpus.indices.foreach(
      index => {
      val timeCurve = corpus(index)
      allBucketSets.foreach(bktSetNo => {
        val bucketNumber = hash(bktSetNo, timeCurve)
        bucketBuilders(bktSetNo)(bucketNumber) += index
      })
    })
    
    val buckets = bucketBuilders.map(_.map(_.result.toSet).toArray).toArray
    
    /*
    println("Bucket overview:")
    buckets.foreach {
      bset => println("Set::");
      bset.foreach {
        bkt => println("  B: "+ bkt.size)
      }
    }
    */

    buckets
  }

  def hash(bktSetNo:Int, curve: TimeCurve): Int = { 
    var mask = 0

    hashPlanes(bktSetNo).foreach(bkt => {
      mask <<= 1
      if(curve.classify(bkt)) {
        mask |= 1
      }
    })

    mask
  }

  def run(query: String, numResults: Int) = {
    val queryCurve = curveMaker.search(query)
    var rl = new RankedList(numResults)

    val data = allBucketSets.map(bktSetNo => {
      hashBuckets(bktSetNo)(hash(bktSetNo, queryCurve))
    }).foldLeft(Set.empty[Int])(_ | _)

    println("Reduced Search Set: " + data.size + " / " + corpus.size)

    data.foreach(index => {
      val timeCurve = corpus(index)
      rl.insert(ScoredTerm(timeCurve.term, queryCurve.score(timeCurve)))
    })

    rl.done
  }
  
  def name = "Curve-based LSH, " + hashBuckets.size + "x" + hashBuckets(0).size + " buckets!"
}



