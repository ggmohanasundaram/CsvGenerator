package au.com.octo.metadata

import org.apache.spark.sql.DataFrame

trait MetaData {

  val inputPath: String

  val metaDataAsDataFrame: DataFrame

  val ddlList: Array[DdlInfo]

}
