package au.com.octo.metadata

import au.com.octo.utils.exception.CsvGeneratorException
import au.com.octo.utils.spark.SparkIo
import org.apache.commons.lang.exception.ExceptionUtils
import org.apache.spark.sql.{DataFrame, SparkSession}

class MetaDataDecorator(override val inputPath: String, val sparkIo: SparkIo)(implicit val sparkSession: SparkSession) extends MetaData {

  override lazy val metaDataAsDataFrame: DataFrame =
    sparkIo.readCSV(inputPath, multiLine = false, header = false, delimiter = ",") match {
      case Right(df) => df.take(1).isEmpty match {
        case true => throw CsvGeneratorException(s"MetaData is Empty from input path $inputPath")
        case false => df

      }
      case Left(e) => throw CsvGeneratorException(s"Exception while Reading MetaData from input path $inputPath")
    }

  /**
    * @return
    * This Method Parse the meta data and convert in DDLInfo like Field Name, starting index, endingIndex and DataType
    */
  override lazy val ddlList: Array[DdlInfo] = {
    try {
      val ddlInfoList = metaDataAsDataFrame.collect().foldLeft(0, Array[DdlInfo]()) {
        (a, row) => {
          val newIndex = a._1 + row.getString(1).toInt
          (newIndex, a._2 :+ new DdlInfo(column_name = row.getString(0),
            length = row.getString(1).toInt, beginIndex = a._1,
            endIndex = newIndex, dataType = row.getString(2)))
        }
      }._2
      ddlInfoList
    }
    catch {
      case e: Exception => throw CsvGeneratorException(s"Meta Data Error ${ExceptionUtils.getStackTrace(e)}")
    }
  }
}
