package au.com.octo.csv

import au.com.octo.metadata.{MetaData, TypeRule}
import au.com.octo.utils.exception.CsvGeneratorException
import au.com.octo.utils.spark.SparkIo
import org.apache.commons.lang.exception.ExceptionUtils
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.slf4j.LoggerFactory

class CSVGenerator(override val inputPath: String,
                   override val metaData: MetaData,
                   override val outputPath: String,
                   override val outputName: String,
                   sparkIo: SparkIo)(implicit sparkSession: SparkSession) extends FileGenerator {

  private val logger = LoggerFactory.getLogger(classOf[CSVGenerator])

  override lazy val rawDataAsDataFrame: DataFrame = sparkIo.readText(inputPath) match {
    case Right(df) => df.toDF("data")
    case Left(e) => throw CsvGeneratorException(s"Exception while Reading raw from input path $inputPath")
  }

  /**
    * This Method Generates Schema from Rule along with a column called Error. This Error column is used to
    * store the error details while processing the Raw Data
    *
    * @param typeRules
    * @return StructType
    */
  private[csv] def getSchemaFromRule(typeRules: Seq[TypeRule]): StructType = {
    val structFields = typeRules.map(x => StructField(x.fieldName, x.targetType.get, true))
    StructType(StructField("Error", StringType, true) +: structFields)
  }


  override def createFile() = {
    try {
      val typeRules = metaData.ddlList.map(x => x.asTypeRule)
      implicit val encoder = RowEncoder(getSchemaFromRule(typeRules))
      val processedData = rawDataAsDataFrame.map(row => CSVGenerator.convertIntoRows(row, typeRules))

      val validData = processedData.filter("Error = 'Error'").drop("Error")
      val invalidData = processedData.filter("Error != 'Error'").select("Error")


      //Generate CSV with Valid Data
       sparkIo.writeCSV(validData, s"$outputPath/$outputName/")

      //Generate Error Log
       sparkIo.writeCSV(invalidData, s"$outputPath/${outputName}_err/")
      logger.info(s"Output Files have been return Successfully under $outputPath")
    }
    catch {
      case e: Exception => throw CsvGeneratorException(s"Exception while createFile CSV File ${ExceptionUtils.getStackTrace(e)}")
    }
  }

}

object CSVGenerator {
  def convertIntoRows(row: Row, rules: Array[TypeRule])() = {
    val typedData = rules.foldLeft(List[Any]("Error")) {
      (rows, rule) =>
        rule.targetVal(row, "data") match {
          case Left(e) =>
            val errorRow = rows.updated(0, s"${rows(0).asInstanceOf[String]}, $e")
            errorRow :+ null
          case Right(r) => rows :+ r
        }
    }
    Row.fromSeq(typedData)
  }
}
