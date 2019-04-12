package au.com.octo.utils.spark

import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

object SparkUtil {

  private val log = LoggerFactory.getLogger(SparkUtil.getClass)

  def createSparkSession(appName: String, sparkRuntimeConf: Map[String, String] = Map.empty): SparkSession = {
    SparkSession
      .builder()
      .appName(appName)
      .master("local[*]")
      .getOrCreate()
  }

}
