package ciir.proteus.galago

import org.lemurproject.galago.core.scoring._
import org.lemurproject.galago.core.retrieval._
import query.Node;
import query.StructuredQuery;
import query.NodeParameters
import iterator._
import org.lemurproject.galago.tupleflow.Parameters;

class CountScorer extends ScoringFunction {
  def score(count: Int, length: Int): Double = count.toDouble
}

class RawScoreIter(p: NodeParameters, ls: MovableLengthsIterator, it: MovableCountIterator) extends
ScoringFunctionIterator(p,ls,it) {
  super.setScoringFunction(new CountScorer)
  
  // need to setScoringFunction before this call
  val maxTF = getMaxTF(p, it)

  override def minimumScore = 0.0
  override def maximumScore = maxTF
}

object WordHistory {
  // format a query to use our custom scoring iterator
  def formatQuery(request: String) = {
    val cleanQuery = request.toLowerCase.replaceAll("^a-z0-9"," ").replaceAll("\\s+", " ").trim
    "#feature:class="+classOf[RawScoreIter].getName()+"("+cleanQuery+")"
  }

  def runQuery(retrieval: Retrieval, request: String, count: Int): Tuple2[Node, Array[ScoredDocument]] = {
    val galagoQuery = formatQuery(request)
    
    val searchParams = new Parameters
    searchParams.set("count", count)

    val root = StructuredQuery.parse(galagoQuery)
    val transformed: Node = retrieval.transformQuery(root, searchParams)

    val start = System.currentTimeMillis
    var scored = retrieval.runQuery(transformed, searchParams)
    val finish = System.currentTimeMillis

    if (scored == null) {
      scored = Array[ScoredDocument]()
    }

    printf("[wordhistory,q=%s,ms=%d,nr=%d]\n", request, finish-start, scored.length)

    Tuple2(transformed, scored)
  }
}

