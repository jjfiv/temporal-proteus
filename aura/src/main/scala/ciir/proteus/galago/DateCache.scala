package ciir.proteus.galago

import gnu.trove.map.hash._
import gnu.trove.set.hash._
import org.lemurproject.galago.tupleflow.Parameters
import org.lemurproject.galago.core.retrieval.ScoredDocument
import org.lemurproject.galago.core.retrieval.processing.ScoringContext
import org.lemurproject.galago.core.retrieval.Retrieval
import org.lemurproject.galago.core.retrieval.LocalRetrieval
import ciir.proteus.Util

//  metadata cache - super important to making this run fast
//  if metadata not in kernel 34 seconds to look up 1000 docs
//  if metadata in kernel, 8 seconds to look up 1000 docs
//  if metadata in this hash, 1 ms to look up 1000 docs :)
class DateCache(val fileStore: String, val handler: Handler with Searchable) {
  // for identifying files
  val MagicNumber = 0xdadecace

  var retrieval = handler.retrieval.asInstanceOf[LocalRetrieval]
  var index = retrieval.getIndex
  
  // updated by init
  private var docDates = new TIntIntHashMap()
  private var dateToWordCount = new TIntIntHashMap()
  private var dateToBookCount = new TIntIntHashMap()
  var minDate = 4096
  var maxDate = -1

  val t0 = System.currentTimeMillis
  if(Util.fileExists(fileStore)) {
    loadFromFile(fileStore)
  } else {
    init()
  }
  val tf = System.currentTimeMillis

  println("Init DateCache in "+(tf-t0)+"ms!")
  
  private def fetchDate(name: String):Int = {
    val p = new Parameters
    p.set("terms", false)
    p.set("tags", false)
    p.set("text", false)

    // get date metadata
    val dateStr = retrieval.getDocument(name, p).metadata.get("date")
    if(dateStr == null) {
      Console.printf("WARN: null date for doc `%s'\n", name)
      return -1
    }

    // convert metadata to number, and store
    try {
      return dateStr.toInt
    } catch {
      // Bad date metdata
      case nfe: NumberFormatException => {
        Console.printf("WARN: bad metadata for doc `%s', date=`%s'\n", name, dateStr);
        return -1
      }
    }
  }

  private def initDocDate(id: Int) = {
    var date = -1
    if(!docDates.containsKey(id)) {
      try {
        val name = retrieval.getDocumentName(id)
        date = fetchDate(name)
        docDates.put(id, date)
      } catch {
        // Unknown Document Number exception...
        case e: java.io.IOException => { docDates.put(id, -1) }
      }
    }
    date
  }

  private def init() {
    var lenIter = index.getLengthsIterator()

    val context = new ScoringContext
    lenIter.setContext(context)

    while(!lenIter.isDone) {
      val id = lenIter.currentCandidate
      
      context.document = id
      lenIter.syncTo(id)

      val len = lenIter.getCurrentLength

      if(len > 0) { 
        val date = initDocDate(id)

        if(date != -1 && date < minDate) { minDate = date }
        if(date > maxDate) { maxDate = date }

        dateToWordCount.adjustOrPutValue(date, len, len)
        dateToBookCount.adjustOrPutValue(date, 1, 1)
      }
      lenIter.movePast(id)
    }

    if(fileStore.length != 0) {
      saveToFile(fileStore)
    }
  }

  def dateForDoc(id: Int) = docDates.get(id)
  // uncached
  //def dateForDoc(id: Int) = fetchDate(retrieval.getDocumentName(id))

  def wordCountForDate(date: Int) = dateToWordCount.get(date)
  def bookCountForDate(date: Int) = dateToBookCount.get(date)

  def inspectDateInfo() {
    var minWords, maxWords, minBooks, maxBooks = 0

    for(date <- dateToWordCount.keys()) {
      val words = dateToWordCount.get(date)
      val books = dateToBookCount.get(date)

      if(books < minBooks) { minBooks = books }
      if(books > maxBooks) { maxBooks = books }
      if(words < minWords) { minWords = words }
      if(words > maxWords) { maxWords = words }
    }

    println("Index:")
    println("  numDates: " + dateToWordCount.size)
    printf("  years: [%d,%d]\n", minDate, maxDate)
    printf("  minBooks: %d, maxBooks: %d\n", minBooks, maxBooks)
    printf("  minWords: %d, maxWords: %d\n", minWords, maxWords)
  }

  def domain = minDate to maxDate

  def size = docDates.size

  def saveToFile(fileName: String) {
    var fos = new java.io.FileOutputStream(fileName)
    var dos = new java.io.DataOutputStream(fos)

    try {
      dos.writeInt(MagicNumber)
      dos.writeInt(size)

      for(id <- docDates.keys) {
        val date = docDates.get(id)
        dos.writeInt(id)
        dos.writeInt(date)
        dos.writeInt(dateToWordCount.get(date))
        dos.writeInt(dateToBookCount.get(date))
      }
    } finally {
      fos.close()
    }
  }

  def loadFromFile(fileName: String) {
    var fis = new java.io.FileInputStream(fileName)
    var dis = new java.io.DataInputStream(fis)

    var error = true

    try {
      val magicNum = dis.readInt
      if(magicNum != MagicNumber) {
        printf("Tried to interpret file \"%s\" as a DateCache object, bad Magic Number 0x%x != 0x%x!\n", fileName, magicNum, MagicNumber )
      }

      val count = dis.readInt

      for(i <- 0 until count) {
        val docId = dis.readInt
        val date = dis.readInt
        val wordCount = dis.readInt
        val bookCount = dis.readInt

        if(date != -1 && date < minDate) { minDate = date }
        if(date > maxDate) { maxDate = date }

        docDates.put(docId, date)
        dateToWordCount.put(date, wordCount)
        dateToBookCount.put(date, bookCount)
      }

      error = false
    } finally {
      fis.close()
      
      // if we failed to load, init from index & corpus metadata
      if(error) { init() }
    }
  }
}

