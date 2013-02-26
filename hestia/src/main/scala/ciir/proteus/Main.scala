package ciir.proteus;

import java.io.File
import org.lemurproject.galago.tupleflow.Parameters

object Main {
  def main(argv: Array[String]) {
    val (files, args) = argv.partition { arg => new File(arg).exists() }

    val parameters = new Parameters()
    
    // load all json files given
    files.map(f => {
      parameters.copyFrom(Parameters.parse(new File(f)))
    })

    // read parameters from arguments next
    parameters.copyFrom(new Parameters(args));

    parameters.getString("adapter") match {
      case "galago" => {
        println("Using Galago backend")
      }
      case x => {
        println("Unknown adapter given to hestia:ciir.proteus.Main `"+x+"'")
        sys.exit()
      }
    }

  }
}

