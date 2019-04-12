package au.com.octo.utils.spark

import au.com.octo.utils.exception.CsvGeneratorException
import org.apache.commons.lang.exception.ExceptionUtils
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}
import org.slf4j.LoggerFactory

class SparkIo {

  private val logger = LoggerFactory.getLogger(classOf[SparkIo])

  def readText(filePath: String)(implicit sparkSession: SparkSession): Either[Exception, Dataset[String]] = {
    try {
      Right(sparkSession.read.textFile(filePath))
    }
    catch {
      case e: Exception => logger.error(ExceptionUtils.getStackTrace(e))
        Left(e)
    }
  }

  def readCSV(filePath: String, multiLine: Boolean, header: Boolean = true, delimiter: String = "|")(implicit sparkSession: SparkSession): Either[Exception, DataFrame] = {

    try {
      val dataset = sparkSession
        .read
        .option("header", header.toString)
        .option("delimiter", delimiter)
        .option("multiLine", multiLine.toString)
        .csv(filePath)
      Right(dataset)

    } catch {
      case e: Exception => logger.error(ExceptionUtils.getStackTrace(e))
        Left(e)
    }
  }

  def writeCSV[T](dataset: Dataset[T], filePath: String, saveMode: SaveMode = SaveMode.Overwrite): String = {
    logger.info(s"Writing CSV files at $filePath with saveMode=$saveMode")
    try {
      dataset
        .write
        .option("header", "true")
        .option("delimiter", ",")
        .option("emptyValue", "")
        .mode(saveMode)
        .csv(filePath)
      filePath
    }
    catch {
      case e: Exception => throw CsvGeneratorException(s"Error while writing CSV $filePath with saveMode=$saveMode => ${e.getMessage}")
    }
  }
}
