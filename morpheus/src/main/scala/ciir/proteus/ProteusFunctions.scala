package ciir.proteus

import ciir.proteus.Constants._
import ciir.proteus.ProteusServlet._
import scala.util.matching.Regex._
import java.util.Calendar

object ProteusFunctions {
  val kReturnableTypes = List("page", 
			      "collection",
			      "person", 
			      "location", 
			      "miscellaneous",
			      "picture",
			      "topic")

  def pluralDisplayName(typeString : String) : String = 
    typeString.toLowerCase match {
      case "collection" => "Books"
      case "location" => "Locations"
      case "miscellaneous" => "Misc"
      case "page" => "Pages"
      case "person" => "People"
      case "topic" => "Topics"
      case "picture" => "Pictures"
      case _ => typeString.capitalize
  }

  def singleDisplayName(typeString : String) : String = 
    typeString.toLowerCase match {
      case "collection" => "Book"
      case _ => typeString.capitalize
  }

  def displayId(aid: AccessIdentifier) : String = 
    String.format("%s--s%s--s%s", aid.identifier, aid.`type`.name, aid.resourceId) 

  def externalId(did: String) : AccessIdentifier = {
    val Array(id, t, r) = did.split("--s")
    AccessIdentifier(identifier = id, 
		     `type` = ProteusType.valueOf(t).get,
		     resourceId = r)
  }

  def getContainingTypes(typeName: String) : Set[String] = {
    val normalized = typeName.toLowerCase
    if (isContainedBy.contains(normalized)) {
      isContainedBy(normalized).toSet & kReturnableTypes.toSet      
    } else {
      Set[String]()
    }
  }

  def getContainedTypes(typeName: String) : Set[String] = {
    val normalized = typeName.toLowerCase
    if (contains.contains(normalized)) {
      contains(normalized).toSet & kReturnableTypes.toSet      
    } else {
      Set[String]()
    }
  }

  val archivePattern = """(archive\.org)""".r
  val wikipediaPattern = """(wikipedia\.org)""".r
  def getExternalLinkText(url: String) : String = url match {
    case archivePattern(url) => "[View at Internet Archive]"
    case wikipediaPattern(url) => "[Search Wikipedia]"
    case _ => "[View external Source]"
  }

  val openPattern = """<(\w+)>""".r
  val closePattern = """</(\w+)>""".r
  def replaceNLPTagsForViewing(rawString: String, itemName: String) : String = {
    val backReplaced = closePattern.replaceAllIn(rawString, (m : Match) => "</span>")
    val frontReplaced = openPattern.replaceAllIn(backReplaced, (m: Match) =>
      if (m.group(1) == "text") {
	"<span>"
      } else {
	m.group(1) match {
	  case "per" => """<span style="color: DarkRed">"""
	  case "loc" => """<span style="color: Navy">"""
	  case "org" => """<span style="color: LightSeaGreen">"""
	  case "misc" => """<span style="color: GoldenRod">"""
	}
      })
    return ("(?i)"+itemName).r.replaceAllIn(frontReplaced, (m: Match) => String.format("<b>%s</b>", m.group(0)))
  }

  private def wordHistoryQueryToJS(q: String, res: Seq[WordHistoryRecord]): String = {
    //sort the data by year, increasing, and output it for Javascript
    
    val data = for (r <- res.sortBy(_.year)) yield {
      "{id:\""+r.docName+"\",year:"+r.year+",weight:"+r.weight+"}"
    }

    "{name:\""+q+"\",raw:["+data.mkString(",")+"]}"
  }

  def wordHistoryResultsToJS(qr: Seq[TermHistory]): String = {
    val objs = for( TermHistory(q, res) <- qr ) yield { wordHistoryQueryToJS(q, res) }

    "["+objs.mkString(",")+"]"
  }

  def normalizeText(name:String):String = {
    val replacedName = replaceChars(name)
    val lower = replacedName.toLowerCase
    val symbolsToSpace = lower.replaceAll("[^a-z01-9 ]", " ").replaceAll("\\s+", " ").trim
    symbolsToSpace
  }
  
  def replaceChars(word: String) : String = word match {
        case "-LRB-" => "("
        case "-RRB-" => ")"
        case "-RSB-" => "]"
        case "-LSB-" => "["
        case "-LCB-" => "{"
        case "-RCB-" => "}"
        case x => x
  }
}
