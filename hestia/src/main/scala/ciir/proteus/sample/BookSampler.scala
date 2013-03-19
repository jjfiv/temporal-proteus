package ciir.proteus.sample

import ciir.proteus._
import ciir.proteus.galago.{Handler, Searchable}
import org.lemurproject.galago.core.index.Index
import org.lemurproject.galago.tupleflow.Parameters
import org.lemurproject.galago.core.retrieval.processing.ScoringContext
import org.lemurproject.galago.core.retrieval.{Retrieval, LocalRetrieval}

object BookSampler {
  def stripXML(s: String) = {
    var sb = new StringBuilder
    var i=0;
    var inTag = false
    while(i < s.size) {
      val cur = s.charAt(i)
      if(cur == '<') {
        inTag = true;
      }
      if(!inTag) {
        sb += cur
      }
      if(cur == '>') {
        inTag = false
      }
      i+=1
    }
    sb.result
  }

  def trimSpaces(s: String) = s.split("\\s").mkString(" ")

  def run(args: Array[String]) {
    val parameters = Util.argsAsJSON(args)

    assert(trimSpaces(stripXML("<tag>a </tag>")) == "a")

    if(parameters.getString("siteId").isEmpty) {
      println("Bad configuration file?")
      sys.exit(-1)
    }

    val handlerType = "collection"
    val handlerParms = parameters.getMap("handlers").getMap(handlerType)
    handlerParms.set("siteId", parameters.getString("siteId"))
    val handler = Handler(ProteusType.valueOf(handlerType).get, handlerParms).get.asInstanceOf[Handler with Searchable]
    val retrieval = handler.retrieval.asInstanceOf[LocalRetrieval]
    val index = retrieval.getIndex

    var lines = Vector.newBuilder[String]

    var kept = 0;
    GalagoIndexUtil.forDocInIndex(retrieval.getIndex, id => {
      if(kept <= 150) {
        try {
          val name = retrieval.getDocumentName(id)

          val p = new Parameters
          p.set("terms", false)
          p.set("tags", false)

          val document = retrieval.getDocument(name, p)
          val metadata = document.metadata
          val title = metadata.get("title")
          val sentences = document.text.split("\\.")
          val mid = sentences.size / 2
          val sample = sentences.view(mid,mid+10).mkString(". ")
          lines += "DOC " + name + " \"" + title + "\" " + sample
          kept += 1
        } catch {
          case _ => { }
        }
      }
    })

    /*
    var lenIter = index.getLengthsIterator

    var id = -1;

    while(!lenIter.isDone && kept <= 150) {
      id = lenIter.currentCandidate
      lenIter.syncTo(id)

      try {
        val name = retrieval.getDocumentName(id)


        val p = new Parameters
        p.set("terms", false)
        p.set("tags", false)

        val document = retrieval.getDocument(name, p)
        val metadata = document.metadata
        val title = metadata.get("title")
        val sentences = document.text.split("\\.")
        val mid = sentences.size / 2
        val sample = sentences.view(mid,mid+10).mkString(". ")
        lines += "DOC " + name + " \"" + title + "\" " + sample
        kept += 1

      } catch {
        case _ => { }
        }

      lenIter.movePast(id)
    }
    */

    Util.printToFile("cs187books.txt", out => {
      lines.result.map(str => out.println(trimSpaces(stripXML(str))))
    })
  }
}
