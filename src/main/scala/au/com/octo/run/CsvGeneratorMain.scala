package au.com.octo.run

import au.com.octo.csv.CSVGenerator
import au.com.octo.metadata.{MetaData, MetaDataDecorator}
import au.com.octo.utils.spark.{SparkIo, SparkUtil}
import org.apache.commons.lang.exception.ExceptionUtils
import org.slf4j.LoggerFactory

object CsvGeneratorMain {

  private val log = LoggerFactory.getLogger(CsvGeneratorMain.getClass)

  def main(args: Array[String]): Unit = {

    val usage =
      """
      Usage:
      sbt "run --metadata <Path>  --rawdata <path> --outputPath <path> --outputName <name> "
      """
    if (args.length == 0) {
      println("Wrong number of input arguments")
      println(usage)
    }
    else {
      val additionalOptions = RunnerUtil.nextAdditionalOptions(Map(), args.toList)
      val metaDataPath = additionalOptions.get('metaDataPath)
      val rawDataPath = additionalOptions.get('rawDataPath)
      val outputPath = additionalOptions.get('outputPath).getOrElse(s"${System.getProperty("user.dir")}/target/output")
      val outputName = additionalOptions.get('outputName)
      assert(!metaDataPath.isEmpty, s"Please Specify Metadata Path $usage")
      assert(!rawDataPath.isEmpty, s"Please Specify rawDataPath Path $usage")
      assert(!outputName.isEmpty, s"Please Specify outputName Path $usage")

      fileGenerator(metaDataPath.get.trim, rawDataPath.get.trim, outputPath, outputName.get.trim)
    }
  }

  def fileGenerator(metaDataPath: String, rawDataPath: String, outputPath: String, outputName: String): Unit = {
    implicit val sparkSession = SparkUtil.createSparkSession("FileGenerator")
    try {

      val sparkIo = new SparkIo
      val metaData: MetaData = new MetaDataDecorator(metaDataPath, sparkIo)
      val fileGenerator: CSVGenerator = new CSVGenerator(rawDataPath, metaData, outputPath, outputName, sparkIo)
      fileGenerator.createFile()
    }
    catch {
      case e: Exception => log.error(s" Exception in fileGenerator ${ExceptionUtils.getStackTrace(e)}")
        throw e
    }
    finally {
      sparkSession.stop()
    }
  }
}
