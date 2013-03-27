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
  val numBucketSets = 3
  var randomizer = new util.Random(13)

  def allBucketSets = 0 until numBucketSets 
  def allBucketBits = 0 until numBits
  def allBuckets = 0 until (1 << numBits)
  
  // generate random planes with max values
  val maxValues = curveMaker.maxArray
  
  
  // generate curve using max values (leads to more uneven buckets, but try this with more than one query)
  def generateRandomCurve(index: Int) = maxValues.map(maxForYear => if (maxForYear == 0) 1 else (randomizer.nextInt % maxForYear))
  
  //def generateRandomCurve(index: Int) = maxValues.map(ignored => randomizer.nextInt)
  
  val hashPlanes: IndexedSeq[IndexedSeq[TimeCurve]] = allBucketSets.map(ignored => {
    allBucketBits.map(index => {
      curveMaker.fromData("hash"+index, generateRandomCurve(index).toArray)
    })
  })

  val hashBuckets: Array[Array[Set[Int]]] = buildBucketsFromCorpus()
  
  private def buildBucketsFromCorpus() = {
    // make numBucketSets * (2^numBits) bucket builders...
    var bucketBuilders = allBucketSets.map(bktsetno => {
      allBuckets.map(bktno => Array.newBuilder[Int])
    })

    corpus.zipWithIndex.foreach({ case Tuple2(timeCurve, index) => {
      allBucketSets.foreach(bktSetNo => {
        val bucketNumber = hash(bktSetNo, timeCurve)
        assert(bucketNumber >= 0 && bucketNumber < (1 << numBits))
        bucketBuilders(bktSetNo)(bucketNumber) += index
      })
    }})
    
    val buckets = bucketBuilders.map(_.map(_.result.toSet).toArray).toArray
    
    println("Bucket overview:")
    buckets.foreach {
      bset => println("Set::");
      bset.foreach {
        bkt => println("  B: "+ bkt.size)
      }
    }

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

  def hash(bktSetNo:Int, curve: TimeCurve) = { 
    boolsToIntBits(hashPlanes(bktSetNo).map(_.classify(curve)))
  }

  def run(query: String, numResults: Int) = {
    val queryCurve = curveMaker.search(query)
    var rl = new RankedList(numResults)


    val data = allBucketSets.map(bktSetNo => {
      hashBuckets(bktSetNo)(hash(bktSetNo, queryCurve))
    }).foldLeft(Set.empty[Int])((accum, cur) => { accum | cur })

    data.foreach(index => {
      val timeCurve = corpus(index)
      rl.insert(ScoredTerm(timeCurve.term, queryCurve.score(timeCurve)))
    })

    rl.done
  }
  
  def name = "Curve-based LSH, " + hashBuckets.size + "x" + hashBuckets(0).size + " buckets!"
}



