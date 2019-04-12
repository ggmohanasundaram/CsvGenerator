package au.com.octo.csv

import au.com.octo.metadata.MetaData
import org.apache.spark.sql.{DataFrame, Dataset}

trait FileGenerator {

  val inputPath: String

  val outputPath: String

  val outputName : String

  val metaData: MetaData

  val rawDataAsDataFrame: DataFrame

  def createFile()
}
