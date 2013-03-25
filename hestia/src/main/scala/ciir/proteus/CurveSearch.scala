package ciir.proteus

sealed case class ScoredTerm(term: String, score: Double);

trait CurveSearch {
  def run(query: String, numResults: Int): Array[ScoredTerm]
}

class RankedList(val numHits: Int) {
  var results = new Array[ScoredTerm](numHits)
  var count = 0

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
}

class BruteForceSearch(val curveMaker: CurveMaker, val corpus: Array[TimeCurve]) extends CurveSearch {
  def run(query: String, numResults: Int) = {
    val queryCurve = curveMaker.search(query)
    var rl = new RankedList(numResults)

    corpus.foreach( timeCurve => {
      rl.insert(ScoredTerm(timeCurve.term, queryCurve.score(timeCurve)))
    })

    rl.results
  }
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

    corpus.foreach(timeCurve => {
      rl.insert(ScoredTerm(timeCurve.term, queryCurve.score(timeCurve)))
    })

    rl.results
  }
}

