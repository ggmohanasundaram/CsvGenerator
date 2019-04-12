package au.com.octo.metadata

import au.com.octo.testutil.TestUtils
import au.com.octo.utils.exception.CsvGeneratorException
import au.com.octo.utils.spark.SparkIo
import org.specs2.mock.Mockito

class MetaDataDecoratorTest extends TestUtils with Mockito {
  sequential

  import sparkSession.implicits._

  private val inputDataFrame = Seq(("A", "B", "C", "D")).toDF()

  "metaDataAsDataFrame " should {
    "Throw Exception when no meta data Found" in {
      val sparkIo = spy(new SparkIo)
      doReturn(Left(new Exception(""))).when(sparkIo).readCSV(anyString, anyBoolean, anyBoolean, anyString)(anyObject)
      val metaDataDecorator = new MetaDataDecorator("somePath", sparkIo)
      metaDataDecorator.metaDataAsDataFrame must throwA(new CsvGeneratorException("Exception while Reading MetaData from input path somePath"))
      there was one(sparkIo).readCSV(anyString, anyBoolean, anyBoolean, anyString)(anyObject)

      doReturn(Right(sparkSession.emptyDataFrame)).when(sparkIo).readCSV(anyString, anyBoolean, anyBoolean, anyString)(anyObject)
      metaDataDecorator.metaDataAsDataFrame must throwA(new CsvGeneratorException("MetaData is Empty from input path somePath"))
    }
    "should return dataframe" in {
      val sparkIo = spy(new SparkIo)
      doReturn(Right(inputDataFrame)).when(sparkIo).readCSV(anyString, anyBoolean, anyBoolean, anyString)(anyObject)
      val metaDataDecorator = new MetaDataDecorator("somePath", sparkIo)
      metaDataDecorator.metaDataAsDataFrame mustEqual (inputDataFrame)
      there was one(sparkIo).readCSV(anyString, anyBoolean, anyBoolean, anyString)(anyObject)
    }
  }

  "ddlList " should {
    "Throw Exception for Invalid Schema" in {
      val metaDataDf = Seq(("A", "A", "date"), ("B", "B", "string"), ("C", "C", "numeric")).toDF()
      val sparkIo = spy(new SparkIo)
      val metaDataDecorator = spy(new MetaDataDecorator("somePath", sparkIo))
      doReturn(metaDataDf).when(metaDataDecorator).metaDataAsDataFrame
      metaDataDecorator.ddlList must throwA[CsvGeneratorException]
    }
    "process and return DDLInfo List" in {
      val metaDataDf = Seq(("A", "10", "date"), ("B", "10", "string"), ("C", "10", "numeric")).toDF()
      val sparkIo = spy(new SparkIo)
      val metaDataDecorator = spy(new MetaDataDecorator("somePath", sparkIo))
      doReturn(metaDataDf).when(metaDataDecorator).metaDataAsDataFrame
      val ddlList = metaDataDecorator.ddlList
      ddlList.length must_== 3
      ddlList(0) mustEqual DdlInfo("A", 10, 0, 10, "date")
      ddlList(1) mustEqual DdlInfo("B", 10, 10, 20, "string")
      ddlList(2) mustEqual DdlInfo("C", 10, 20, 30, "numeric")
    }
  }
}
