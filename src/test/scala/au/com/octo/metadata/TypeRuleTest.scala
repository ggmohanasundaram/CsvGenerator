package au.com.octo.metadata

import au.com.octo.testutil.TestUtils
import org.apache.spark.sql.types.{DoubleType, StringType}
import org.specs2.mock.Mockito

class TypeRuleTest extends TestUtils with Mockito {
  sequential

  "BY_SHORTNAME length" in {
    TypeRule.BY_SHORTNAME.size mustEqual (3)
  }

  "case class Str2Str" should {
    "method targetType should return proper DataType" in {
      val str2Str = Str2Str("name", 0, 10)
      str2Str.targetType must_== (Some(StringType))
    }
    "method targetVal" in {
      val str2Str = Str2Str("name", 0, 10)
      str2Str.targetVal(genRow("some", StringType, "A"), "B") must beLeft("B cannot be found")
      str2Str.targetVal(genRow(null, StringType, "A"), "A") must beRight(null: java.lang.String)
      str2Str.targetVal(genRow("someData", StringType, "A"), "A") must beRight("someData")
      str2Str.targetVal(genRow("", StringType, "A"), "A") must beRight(null: java.lang.String)
      str2Str.targetVal(genRow("      ", StringType, "A"), "A") must beRight(null: java.lang.String)
      str2Str.targetVal(genRow(" someData      ", StringType, "A"), "A") must beRight("someData")
      str2Str.targetVal(genRow("  some         Data ", StringType, "A"), "A") must beRight("some")
    }
  }

  "case class Str2Date" should {
    "method targetType should return proper DataType" in {
      val str2Date = Str2Date("name", 0, 10)
      str2Date.targetType must_== (Some(StringType))
    }
    "method targetVal" in {
      val str2Date = Str2Date("name", 0, 10)
      str2Date.targetVal(genRow("some", StringType, "A"), "B") must beLeft("B cannot be found")
      str2Date.targetVal(genRow("someDateInput", StringType, "A"), "A") must beLeft("name someDateIn not an String Date with pattern: yyyy-MM-dd")
      str2Date.targetVal(genRow("31-01-1975", StringType, "A"), "A") must beLeft("name 31-01-1975 not an String Date with pattern: yyyy-MM-dd")
      str2Date.targetVal(genRow("1975-01-31", StringType, "A"), "A") must beRight("31/01/1975")
      str2Date.targetVal(genRow("1975-01-31sample", StringType, "A"), "A") must beRight("31/01/1975")
      str2Date.targetVal(genRow("          ", StringType, "A"), "A") must beRight(null: java.lang.String)
      str2Date.targetVal(genRow(null, StringType, "A"), "A") must beRight(null: java.lang.String)
    }
  }

  "case class Str2Double" should {
    "method targetType should return proper DataType" in {
      val str2Double = Str2Double("name", 0, 10)
      str2Double.targetType must_== (Some(DoubleType))
    }
    "method targetVal" in {
      val str2Double = Str2Double("name", 0, 10)
      str2Double.targetVal(genRow("some", StringType, "A"), "B") must beLeft("B cannot be found")
      str2Double.targetVal(genRow("someDateInput", StringType, "A"), "A") must beLeft("name  someDateIn not a String Decimal")
      str2Double.targetVal(genRow("300.006", StringType, "A"), "A") must beRight(300.006)
      str2Double.targetVal(genRow("-300.006", StringType, "A"), "A") must beRight(-300.006)
      str2Double.targetVal(genRow("     40.04", StringType, "A"), "A") must beRight(40.04)
      str2Double.targetVal(genRow("          ", StringType, "A"), "A") must beRight(null: java.lang.Double)
      str2Double.targetVal(genRow(null, StringType, "A"), "A") must beRight(null: java.lang.Double)
    }
  }
}
