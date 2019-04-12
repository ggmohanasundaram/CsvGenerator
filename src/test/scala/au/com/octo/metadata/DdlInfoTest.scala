package au.com.octo.metadata

import au.com.octo.testutil.TestUtils
import au.com.octo.utils.exception.CsvGeneratorException
import org.specs2.mock.Mockito

class DdlInfoTest extends TestUtils with Mockito {
  sequential

  "Method asTypeRule" should {
    "return execption for unsupported datatype" in {
      val ddl = DdlInfo(column_name = "someColumnName", length = 10, beginIndex = 0, endIndex = 10,
        dataType = "SomeDataType")
      ddl.asTypeRule must throwA(new CsvGeneratorException("SomeDataType not supported"))
    }
    "return type Rule for valid datatype" in {
      val ddlDate = DdlInfo(column_name = "someColumnName", length = 10, beginIndex = 0, endIndex = 10,
        dataType = "date")
      val ddlString = DdlInfo(column_name = "someColumnName", length = 10, beginIndex = 0, endIndex = 10,
        dataType = "string")
      val ddlNemeric = DdlInfo(column_name = "someColumnName", length = 10, beginIndex = 0, endIndex = 10,
        dataType = "numeric")
      ddlDate.asTypeRule.isInstanceOf[Str2Date] mustEqual true
      ddlString.asTypeRule.isInstanceOf[Str2Str] mustEqual true
      ddlNemeric.asTypeRule.isInstanceOf[Str2Double] mustEqual true
    }
  }

  "Method toClassType" should {
    "return class name from TypeRule Map" in {
      val ddlDate = DdlInfo(column_name = "someColumnName", length = 10, beginIndex = 0, endIndex = 10,
        dataType = "date")
      ddlDate.toClassType("data") must beNone
      ddlDate.toClassType("date") mustNotEqual  None
    }
  }

}
