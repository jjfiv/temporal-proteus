package ciir.ttools;

import org.lemurproject.galago.tupleflow._

import edu.stanford.nlp.io._
import edu.stanford.nlp.ling._
import edu.stanford.nlp.pipeline._
import edu.stanford.nlp.trees._
import edu.stanford.nlp.util._

object Main {
  def main(args: Array[String]) {
    val data = "This is a sentence that describes the work that we're doing here in 1995. Ten years later, I'm not sure what we have accomplished."

    val props = new java.util.Properties
    props.put("annotators", "tokenize, ssplit, pos, lemma, ner, parse, dcoref")
    val pipeline = new StanfordCoreNLP(props)
    val doc = new Annotation(data)

    pipeline.annotate(doc);
    pipeline.prettyPrint(doc, new java.io.PrintWriter(System.out))

  }
}

