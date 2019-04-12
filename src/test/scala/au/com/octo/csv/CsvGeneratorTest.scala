package au.com.octo.csv

import au.com.octo.metadata._
import au.com.octo.testutil.TestUtils
import au.com.octo.utils.exception.CsvGeneratorException
import au.com.octo.utils.spark.SparkIo
import org.apache.spark.sql.types.StringType
import org.specs2.mock.Mockito

class CsvGeneratorTest extends TestUtils with Mockito {
  sequential

  import sparkSession.implicits._

  "rawDataAsDataFrame" should {
    "throw exception if no path found" in {
      val metaData = mock[MetaDataDecorator]
      val sparkIo = spy(new SparkIo)
      val csvGenerator = new CSVGenerator("someInputPath", metaData, "someOutPath", "someOutputName", sparkIo)
      doReturn(Left("Exception")).when(sparkIo).readText(anyString)(anyObject)
      csvGenerator.rawDataAsDataFrame must throwA(new CsvGeneratorException("Exception while Reading raw from input path someInputPath"))
      there was one(sparkIo).readText(anyString)(anyObject)
    }
    "return raw data as dataframe" in {
      val dataFrame = Seq("A", "B", "C").toDF("data")
      val metaData = mock[MetaDataDecorator]
      val sparkIo = spy(new SparkIo)
      val csvGenerator = new CSVGenerator("someInputPath", metaData, "someOutPath", "someOutputName", sparkIo)
      doReturn(Right(dataFrame)).when(sparkIo).readText(anyString)(anyObject)
      csvGenerator.rawDataAsDataFrame.columns(0) must_== ("data")
    }
  }

  "getSchemaFromRule should retrun proper schema" in {
    val metaData = mock[MetaDataDecorator]
    val sparkIo = spy(new SparkIo)
    val csvGenerator = new CSVGenerator("someInputPath", metaData, "someOutPath", "someOutputName", sparkIo)
    val a = Str2Str("A", 10, 20)
    val b = Str2Date("A", 10, 20)
    val c = Str2Double("A", 10, 20)
    val structType = csvGenerator.getSchemaFromRule(Seq(a, b, c))
    structType.mkString("") must_== "StructField(Error,StringType,true)StructField(A,StringType,true)StructField(A,StringType,true)StructField(A,DoubleType,true)"
  }

  "convertIntoRows should return error or valid Rows" in {
    val validRow = genRow("1975-01-31sundaram\"       61.1", StringType, "data")
    val invalidRow = genRow("1975/01/31sundaram\"       61.1", StringType, "data")
    val a = Str2Date("A", 1, 10)
    val b = Str2Str("B", 10, 20)
    val c = Str2Double("C", 20, 30)
    CSVGenerator.convertIntoRows(validRow, Array(b, a, c)).mkString(" ") must_== "Error sundaram\" 26/01/0975 61.1"
    CSVGenerator.convertIntoRows(invalidRow, Array(b, a, c)).mkString(" ") must_== "Error, A 975/01/31 not an String Date with pattern: yyyy-MM-dd sundaram\" null 61.1"
  }

  "createFile should write error and valid csv" in {
    val dataFrame = Seq("A", "B", "C").toDF("data")
    val a = Str2Str("A", 10, 20)
    val b = Str2Date("A", 10, 20)
    val c = Str2Double("A", 10, 20)
    val typeRules = Seq(a, b, c)
    val metaData = mock[MetaDataDecorator]
    val sparkIo = spy(new SparkIo)
    val csvGenerator = spy(new CSVGenerator("someInputPath", metaData, "someOutPath", "someOutputName", sparkIo))
    doReturn(Right(dataFrame)).when(sparkIo).readText(anyString)(anyObject)
    doReturn(Array(DdlInfo(column_name = "someColumnName", length = 10, beginIndex = 0, endIndex = 10, dataType = "date"))).when(metaData).ddlList
    csvGenerator.createFile()
    there was two(sparkIo).writeCSV(anyObject,anyObject,anyObject)
  }
}
