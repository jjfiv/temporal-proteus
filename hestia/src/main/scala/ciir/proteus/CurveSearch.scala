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

class SampledSearch(val curveMaker: CurveMaker, val corpus: Array[TimeCurve]) extends CurveSearch {
  val CurveDist = 10

  def run(query: String, numResults: Int) = {
    val queryCurve = curveMaker.search(query)
    var rl = new RankedList(numResults)

    var i=0
    val len = corpus.size
    while(i < len) {
      if(i % CurveDist == 0) {
        val timeCurve = corpus(i)
        rl.insert(ScoredTerm(timeCurve.term, queryCurve.score(timeCurve)))
      }
      i+=1
    }

    rl.done
  }

  def name = "Sampled (every " + CurveDist + ") Search"
}
class BinaryLSHSearch(val curveMaker: CurveMaker, val corpus: Array[TimeCurve]) extends CurveSearch {
  var randomizer = new util.Random(13)
  val randomPlane = curveMaker.fromData("hash0", curveMaker.domain.map(x => randomizer.nextInt).toArray)

  val (vocabTrue, vocabFalse) = Util.timed("BinaryLSH", {
    corpus.partition(hash)
  })

  def hash(curve: TimeCurve) = { curve.classify(randomPlane) }

  def run(query: String, numResults: Int) = {
    val queryCurve = curveMaker.search(query)
    var rl = new RankedList(numResults)

    val data = if (hash(queryCurve)) { vocabTrue } else { vocabFalse }

    data.foreach(timeCurve => {
      rl.insert(ScoredTerm(timeCurve.term, queryCurve.score(timeCurve)))
    })

    rl.done
  }

  def name = "Binary LSH - only two buckets, no overlap"
}

class LSHSearch(val curveMaker: CurveMaker, val corpus: Array[TimeCurve]) extends CurveSearch {
  val numBits = 4
  var randomizer = new util.Random(13)
  
  // generate random planes
  val hashPlanes = (0 until numBits).map(index => {
    curveMaker.fromData("hash"+index, curveMaker.domain.map(x => randomizer.nextInt).toArray)
  })

  val hashBuckets = buildBucketsFromCorpus()
  
  private def buildBucketsFromCorpus() = {
    // make 2^numBits bucket builders...
    var bucketBuilders = (0 until (1 << numBits)).map(x => Array.newBuilder[TimeCurve])

    corpus.foreach(timeCurve => {
      val bucketNumber = hash(timeCurve)
      assert(bucketNumber >= 0 && bucketNumber < (1 << numBits))
      bucketBuilders(bucketNumber) += timeCurve
    })
    
    val buckets = bucketBuilders.map(_.result).toArray
    println("Bucket overview:")
    buckets.foreach( bkt => println("B: "+ bkt.size) )

    buckets
  }

  def boolsToIntBits(bools: Seq[Boolean]) = {
    assert(bools.size == numBits)
    var mask = 0
    bools.foreach(b => {
      mask <<= 1;
      mask |= (if (b) 1 else 0)
    })
    mask
  }

  def hash(curve: TimeCurve) = { 
    boolsToIntBits(hashPlanes.map(_.classify(curve)))
  }

  def run(query: String, numResults: Int) = {
    val queryCurve = curveMaker.search(query)
    var rl = new RankedList(numResults)

    val data = hashBuckets(hash(queryCurve))

    data.foreach(timeCurve => {
      rl.insert(ScoredTerm(timeCurve.term, queryCurve.score(timeCurve)))
    })

    rl.done
  }
  
  def name = "Curve-based LSH, " + hashBuckets.size + " buckets!"
}



