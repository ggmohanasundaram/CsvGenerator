package au.com.octo.testutil

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.slf4j.{Logger, LoggerFactory}
import org.specs2.mutable.Specification
import org.specs2.specification.AfterAll

trait TestUtils extends Specification with AfterAll {

  val logger: Logger = LoggerFactory.getLogger(classOf[TestUtils])

  implicit lazy val sparkSession: SparkSession = {
    logger.info("Creating SparkSession for tests")

    SparkSession
      .builder()
      .appName(getClass.getName)
      .master("local[*]")
      .getOrCreate()
  }

  def genRow(v: Any, dataType: DataType, fieldName: String = "someFieldName") =
    new GenericRowWithSchema(Array(v), new StructType(Array(StructField(fieldName, dataType))))

  def afterAll: Unit = {
    logger.info("SparkSession stopped !")
    sparkSession.stop()
    logger.info(s"SparkSession is stopped: " + sparkSession.sparkContext.isStopped)
  }
}

