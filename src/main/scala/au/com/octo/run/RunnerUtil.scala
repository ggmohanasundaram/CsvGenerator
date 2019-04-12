package au.com.octo.run

import org.slf4j.LoggerFactory

object RunnerUtil {

  private val logger = LoggerFactory.getLogger(RunnerUtil.getClass)

  type OptionMap = Map[Symbol, String]

  def nextAdditionalOptions(optionsMap: OptionMap, args: List[String]): OptionMap = {
    args match {
      case Nil => optionsMap
      case "--metadata" :: value :: tail =>
        nextAdditionalOptions(optionsMap ++ Map('metaDataPath -> value.toString), tail)
      case "--rawdata" :: value :: tail =>
        nextAdditionalOptions(optionsMap ++ Map('rawDataPath -> value.toString), tail)
      case "--outputPath" :: value :: tail =>
        nextAdditionalOptions(optionsMap ++ Map('outputPath -> value.toString), tail)
      case "--outputName" :: value :: tail =>
        nextAdditionalOptions(optionsMap ++ Map('outputName -> value.toString), tail)
      case option :: _ => println("Unknown option " + option)
        sys.exit(1)
    }
  }
}