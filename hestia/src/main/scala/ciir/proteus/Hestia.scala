package ciir.proteus;

import java.io.File
import org.lemurproject.galago.tupleflow.Parameters

object Hestia {
  def argsAsJSON(argv: Array[String]) = {
    val (files, args) = argv.partition { arg => new File(arg).exists() }

    val parameters = new Parameters()
    
    // load all json files given
    files.map(f => {
      parameters.copyFrom(Parameters.parse(new File(f)))
    })

    // read parameters from arguments next
    parameters.copyFrom(new Parameters(args));

    parameters
  }

}

object SoundexAlgorithm {
  val precomputed = "abcdefghijklmnopqrstuvwxyz".map(soundexNumber)

  // call toLower / isLetter before using this method
  private def getNum(ch: Char): Char = {
    try {
      return precomputed.charAt(ch.toByte - 'a')
    } catch {
      case x: Exception => { 
        println("Error on getNum of character '"+ch+"'")
        return '0'
      }
    }
  }

  private def soundexNumber(ch: Char): Char = ch match {
    case 'a' | 'e' | 'i' | 'o' | 'u' | 'y' | 'h' | 'w' => '0'
    case 'b' | 'f' | 'p' | 'v' => '1'
    case 'c' | 'g' | 'j' | 'k' | 'q' | 's' | 'x' | 'z' => '2'
    case 'd' | 't' => '3'
    case 'l' => '4'
    case 'm' | 'n' => '5'
    case 'r' => '6'
    case _ => '0'
  }

  private def isVowel(ch : Char) = ch match {
    case 'a' | 'e' | 'i' | 'o' | 'u' => true
    case _ => false
  }

  private def normalize(ch: Char): Char = { if(ch.isLetter) ch else 'a' }

  def apply(in: String) = {
    assert(!in.isEmpty)
    var sb = new StringBuilder
    sb += in.head.toLower

    var lastNum = getNum(normalize(sb.last))
    var lastVowel = false
    var lastHW = false
    
    in.tail.foreach(ch => {
      if(sb.length < 4) {
        val letter = normalize(ch)
        val num = getNum(letter)
        val prevNumSame = lastNum == num

        //println("proc '"+letter+" num="+num+"', hw="+lastHW+" prev="+lastNum)
        if (num == '0') {
          // drop this character
        } else if (prevNumSame && (lastHW || !lastVowel)) {
          // drop this character
        } else {
          sb += num
          lastNum = num
        }
        lastVowel = isVowel(letter)
        lastHW = (letter == 'h' || letter == 'w')
      }
    })
    
    if(sb.length > 4) {
      sb.setLength(4)
    } else {
      while(sb.length < 4) {
        sb += ' '
      }
    }
    //println(sb.result)
    sb.result
  }

  def test() {
    // algorithm, examples taken from Wikipedia
    assert(apply("Robert") == "r163")
    assert(apply("Robert") == apply("Rupert"))
    assert(apply("Ashcraft") == "a261")
    assert(apply("Tymczak") == "t522")
    assert(apply("Pfister") == "p236")
  }
}

// use the date data to read in the set of terms in the corpus
class CorpusWordSet(parameters: Parameters) {
  import org.lemurproject.galago.core.index.disk.DiskBTreeReader
  import gnu.trove.map.hash.TObjectIntHashMap

  //val path = parameters.getString("dateDirectory")
  val path = parameters.getMap("handlers").getMap("collection").getString("index")

  val dfile = new File(path, "postings")
  val index = new DiskBTreeReader(dfile)
  printf("Opening word frequency index at %s\n", dfile.getCanonicalPath)

  def process() {
    var numTerms = 0
    var uniqTerms = 0
    var numTermsWithSpaces = 0
    val iter = index.getIterator()

    val smap = new TObjectIntHashMap[String]()
    
    var i=0
    var soundexSamples = 0

    while(!iter.isDone()) {
      val bytesOfKey = iter.getKey
      val hasSpace = bytesOfKey.exists(_<=32) 

      // soundexify everything that passes through
      val s = new String(bytesOfKey)
      smap.adjustOrPutValue(SoundexAlgorithm(s), 1, 1)
      soundexSamples += 1

      val stream = iter.getSubValueStream(0, iter.getValueLength)
      val count = stream.readInt()
      iter.nextKey();
      
      i+=1 
      uniqTerms+=1
      numTerms+=count
      if(hasSpace) {
        numTermsWithSpaces+=1
      }
    }

    println("Unique Terms: "+uniqTerms);
    println("Total Terms: "+numTerms);
    println("Num Terms with Spaces:" + numTermsWithSpaces)
    println("Soundex Samples:" + soundexSamples)
    println("Soundex Equivalence Classes:" + smap.size())
  }

}


object SoundexAnalyzer {
  def main(argv: Array[String]) {
    SoundexAlgorithm.test()

    val parameters = Hestia.argsAsJSON(argv)

    parameters.getString("adapter") match {
      case "galago" => {
        println("Using Galago backend")
      }
      case x => {
        println("Unknown adapter given to ciir.proteus.Hestia `"+x+"'")
        sys.exit()
      }
    }

    val corpus = new CorpusWordSet(parameters)
    corpus.process()
  }


}

