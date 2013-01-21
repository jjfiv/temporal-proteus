package ciir.proteus.galago

import org.lemurproject.galago.core.scoring._
import org.lemurproject.galago.core.retrieval._
import query.NodeParameters
import iterator._

class IdentityScorer extends ScoringFunction {
  def score(count: Int, length: Int): Double = count.toDouble
}

class IdentityScoringIterator(p: NodeParameters, ls: MovableLengthsIterator, it: MovableCountIterator) extends
ScoringFunctionIterator(p,ls,it) {
  super.setScoringFunction(new IdentityScorer)
  
  // need to setScoringFunction before this call
  val maxTF = getMaxTF(p, it)

  override def minimumScore = 0.0
  override def maximumScore = maxTF
}

