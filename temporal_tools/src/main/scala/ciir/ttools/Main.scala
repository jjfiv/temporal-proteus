package ciir.ttools;

import org.lemurproject.galago.tupleflow.Parameters
import org.lemurproject.galago.tupleflow.StreamCreator
import org.lemurproject.galago.core.parse.Document
import org.lemurproject.galago.core.types.DocumentSplit
import ciir.proteus.galago.parse._

import java.io.BufferedInputStream
import java.util.zip.GZIPInputStream
import javax.xml.stream.XMLInputFactory
import javax.xml.stream.XMLStreamConstants
import javax.xml.stream.XMLStreamException
import javax.xml.stream.util.StreamReaderDelegate

object Main {
  def analyze(text: String) {
    // stanford NLP
    import edu.stanford.nlp.io._
    import edu.stanford.nlp.ling._
    import edu.stanford.nlp.pipeline._
    import edu.stanford.nlp.trees._
    import edu.stanford.nlp.util._

    val props = new java.util.Properties
    props.put("annotators", "tokenize, ssplit, pos, lemma, ner, parse, dcoref")
    val pipeline = new StanfordCoreNLP(props)
    val doc = new Annotation(text)

    pipeline.annotate(doc);
    pipeline.prettyPrint(doc, new java.io.PrintWriter(System.out))

  }

  def main(args: Array[String]) {
    val book = //"/home/jfoley/Desktop/data/linked-books/data/00/83/cranberriesnatio4951port/cranberriesnatio4951port_mbtei_linked.xml.gz"
"/home/jfoley/Desktop/data/linked-books/data/00/6a/consolidationoff02unit/consolidationoff02unit_mbtei_linked.xml.gz"
    val reader = makeXMLReader(book)

    while(true) {
      if(!reader.hasNext()) {
        return;
      }

      try {
        val status = reader.next();
        status match {
          case XMLStreamConstants.START_ELEMENT => {
            println("<"+reader.getLocalName()+">")
          }
          case XMLStreamConstants.END_ELEMENT => {
            println("</"+reader.getLocalName()+">")
          }
          case XMLStreamConstants.CHARACTERS => {
            println(reader.getText());
          }
        }
      } catch {
        case e: Exception => {
          System.err.println(e.getMessage())
          e.printStackTrace(System.err)
          System.out.flush()
          return
        }
      }
    }

  }

  def makeXMLReader(bookPath: String) = {
    val factory = XMLInputFactory.newInstance()
    factory.setProperty(XMLInputFactory.IS_COALESCING, true)

    val stream = getGZIPStream(bookPath)

    new StreamReaderDelegate(factory.createXMLStreamReader(stream))
  }

  def getGZIPStream(bookPath: String) = {
    new BufferedInputStream(new GZIPInputStream(StreamCreator.realInputStream(bookPath)))
  }
}

