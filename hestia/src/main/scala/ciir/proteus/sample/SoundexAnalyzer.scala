package ciir.proteus.sample;

import java.io.File
import org.lemurproject.galago.tupleflow.Parameters
import ciir.proteus.Util

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

  def regular(in: String) = {
    var sb = new StringBuilder(long(in));
    
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

  def long(in: String) = {
    assert(!in.isEmpty)
    var sb = new StringBuilder
    sb += in.head.toLower

    var lastNum = getNum(normalize(sb.last))
    var lastVowel = false
    var lastHW = false
    
    in.tail.foreach(ch => {
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
    })
    
    sb.result
  }

  def test() {
    // algorithm, examples taken from Wikipedia
    assert(regular("Robert") == "r163")
    assert(regular("Robert") == regular("Rupert"))
    assert(regular("Ashcraft") == "a261")
    assert(regular("Tymczak") == "t522")
    assert(regular("Pfister") == "p236")
  }
}

// use the date data to read in the set of terms in the corpus
class CorpusWordSet(parameters: Parameters) {
  import org.lemurproject.galago.core.index.disk.DiskBTreeReader
  import org.lemurproject.galago.tupleflow.VByteInput;
  import gnu.trove.map.hash._

  val path = parameters.getString("dateDirectory")
  //val path = parameters.getMap("handlers").getMap("collection").getString("index")

  val dfile = new File(path, "postings")
  val index = new DiskBTreeReader(dfile)
  printf("Opening word frequency index at %s\n", dfile.getCanonicalPath)

  val bookStats = false

  def process() {
    var numTerms = 0
    var uniqTerms = 0
    var numTermsWithSpaces = 0
    var singleTerms = 0
    val iter = index.getIterator()

    val srMap = new TObjectIntHashMap[String]()
    val slMap = new TObjectIntHashMap[String]()
    val wordsPerDateMap = new TIntIntHashMap()
    val booksPerDateMap = new TIntIntHashMap()
    
    var i=0
    var soundexSamples = 0

    while(!iter.isDone()) {
      val bytesOfKey = iter.getKey
      val hasSpace = bytesOfKey.exists(_<=32) 

      // soundexify everything that passes through
      val s = new String(bytesOfKey)
      if(!s.isEmpty) {
        srMap.adjustOrPutValue(SoundexAlgorithm.regular(s), 1, 1)
        slMap.adjustOrPutValue(SoundexAlgorithm.long(s), 1, 1)
        soundexSamples += 1
      }

      val stream = iter.getSubValueStream(0, iter.getValueLength)
      val count = stream.readInt()

      if(bookStats) {
        val vStream = new VByteInput(stream)
        for(i <- 0 until count) {
          val date = vStream.readInt
          val weight = vStream.readInt

          wordsPerDateMap.adjustOrPutValue(date, weight, weight)
          booksPerDateMap.adjustOrPutValue(date, 1, 1)
        }
      }

      iter.nextKey();
      
      i+=1 
      uniqTerms+=1
      numTerms+=count
      if(count <= 1) {
        singleTerms += 1
      }
      if(hasSpace) {
        numTermsWithSpaces+=1
      }
    }

    def fraction(a: Int, b: Int): Double = a.toDouble/b.toDouble

    printf("Unique Terms: %d\n",uniqTerms);
    printf("Total Terms: %d\n",numTerms);
    printf("Num Terms with Spaces: %d\n", numTermsWithSpaces)
    printf("Terms with tf=1: %d\n", singleTerms)
    printf("Fraction Single Terms: %.3f\n", fraction(singleTerms,uniqTerms))
    
    printf("Soundex Samples: %d\n", soundexSamples)
    printf("Soundex Equivalence Classes. Regular: %d Long: %d\n",
      srMap.size(), slMap.size())
    printf("Mean Words Per Class: Regular: %.3f Long: %.3f\n",
      fraction(soundexSamples, srMap.size()),
      fraction(soundexSamples, slMap.size()))

    if(bookStats) {
      val uniqYears = booksPerDateMap.size()

      printf("Unique Years: %d\n", uniqYears)

      printf("Date, Books, Words\n")
      for(year <- booksPerDateMap.keys()) {
        printf("%4d %8d %8d\n", year, booksPerDateMap.get(year), wordsPerDateMap.get(year))
    }
    }
  }

}


object SoundexAnalyzer {
  def run(argv: Array[String]) {
    SoundexAlgorithm.test()

    val parameters = Util.argsAsJSON(argv)

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


