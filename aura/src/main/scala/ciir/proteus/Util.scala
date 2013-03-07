package ciir.proteus

object Util {
  import java.io._

  def fileExists(fileName: String) = {
    new File(fileName).exists()
  }
}

