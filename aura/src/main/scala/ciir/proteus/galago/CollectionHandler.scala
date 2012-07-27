package ciir.proteus.galago

import java.io.File

import scala.collection.mutable.ListBuffer
import scala.collection.JavaConversions._

import ciir.proteus._
import com.twitter.finagle.builder._
import com.twitter.finagle.thrift.ThriftServerFramedCodec
import com.twitter.util.{Duration,Future}
import org.apache.thrift.protocol._

import org.lemurproject.galago.core.retrieval._
import org.lemurproject.galago.core.parse.Document
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.StructuredQuery;
import org.lemurproject.galago.core.retrieval.RetrievalFactory
import org.lemurproject.galago.core.index.corpus.SnippetGenerator;
import org.lemurproject.galago.tupleflow.Parameters;

object CollectionHandler {
  def apply(p : Parameters) = new CollectionHandler(p)
}

class CollectionHandler(p : Parameters) extends Handler(p) {
  val archiveReaderUrl = "http://archive.org/stream"
  val generator = new SnippetGenerator
  val retrieval = RetrievalFactory.instance(parameters)

  override def search(srequest: SearchRequest): List[SearchResult] = {
    val (root, scored) = runQueryAgainstIndex(srequest)
    if (scored == null) return List()
    val queryTerms = StructuredQuery.findQueryTerms(root);
    generator.setStemming(root.toString().contains("part=stemmedPostings"));
    val c = new Parameters;
    c.set("terms", false);
    c.set("tags", false);
    var results = ListBuffer[SearchResult]()
    for (scoredDocument <- scored) {
      val identifier = scoredDocument.documentName;
      val document = retrieval.getDocument(identifier, c);
      val accessId = AccessIdentifier(identifier = identifier, 
				      `type` = ProteusType.Collection, 
				      resourceId = siteId)
      val summary = ResultSummary(getSummary(document, queryTerms), List())
      val title = if (document.metadata.containsKey("title")) {
        generator.highlight(document.metadata.get("title"), queryTerms);
      } else {
	String.format("No Title (%s)", scoredDocument.documentName)
      }
      val externalUrl = String.format("%s/%s#page/cover/mode/2up",
				      archiveReaderUrl,
				      accessId.identifier)
      var result = SearchResult(id = accessId,
				score = scoredDocument.score,
				title = Some(title),
				summary = Some(summary),
				externalUrl = Some(externalUrl),
				thumbUrl = getThumbUrl(accessId),
				imgUrl = getImgUrl(accessId))
      if (document.metadata.containsKey("url")) {
	result = result.copy(externalUrl = Some(document.metadata.get("url")));
      }
      results += result
    }
    return results.toList
  }

  override def lookup(ids: Set[AccessIdentifier]): List[ProteusObject] = 
    ids.map { id => getCollectionObject(id) }.toList

  val c = new Parameters;
  c.set("terms", true);
  c.set("tags", true);    
  private def getCollectionObject(id: AccessIdentifier) : ProteusObject = {
    val document = retrieval.getDocument(id.identifier, c)
    val title = if (document.metadata.containsKey("title")) {
      document.metadata.get("title");
    } else {
      String.format("No title (%s)", id.identifier)
    }

    var collection = Collection(creators = Seq[String]())
    var pObject = ProteusObject(id = id,
				title = Some(title),
				description = Some("A book"),
				thumbUrl = getImgUrl(id),
				collection = Some(collection))
    return pObject
  }

  private def getSummary(document : Document, 
			 query: java.util.Set[String]) : String = {
    if (document.metadata.containsKey("description")) {
      val description = document.metadata.get("description");
      if (description.length() > 10) {
        return generator.highlight(description, query);
      }
    }
    return generator.getSnippet(document.text, query);
  }

  private def getImgUrl(id: AccessIdentifier) : Some[String] = {
    return Some(String.format("http://www.archive.org/download/%s/page/cover.jpg",
		     id.identifier))
  }

  private def getThumbUrl(id: AccessIdentifier) : Some[String] = {
    return Some(
      String.format("http://www.archive.org/download/%s/page/cover_thumb.jpg",
		    id.identifier))
  }
}