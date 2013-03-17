package ciir.proteus

object Util {
  import java.io._

  def fileExists(fileName: String) = {
    new File(fileName).exists()
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
}

