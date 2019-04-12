package au.com.octo.metadata

import java.text.SimpleDateFormat

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.joda.time.format.DateTimeFormat

import scala.util.{Success, Try}

trait TypeRule {

  type FieldError = String
  val fieldName: String
  val targetType: Option[DataType] = None

  def targetVal(row: Row, rawColumnName: String): Either[FieldError, Any]
}

object TypeRule {
  val BY_SHORTNAME: Map[String, Class[_ <: TypeRule]] = {
    Map("date" -> classOf[Str2Date],
      "numeric" -> classOf[Str2Double],
      "string" -> classOf[Str2Str])
  }
}

case class Str2Date(override val fieldName: String, beginIndex: Int, endIndex: Int) extends TypeRule {

  val inputPattern = "yyyy-MM-dd"

  val outputFormat = "dd/MM/yyyy"

  override val targetType = Some(StringType)

  override def targetVal(row: Row, rawColumnName: String): Either[FieldError, Any] = {
    Try(row.getAs[String](rawColumnName)) match {
      case Success(x) if x == null => Right(null)
      case Success(x) =>
        Try(x.slice(beginIndex, endIndex).trim) match {
          case Success(y) if y.isEmpty => Right(null)
          case Success(y) =>
            val fmt = DateTimeFormat.forPattern(inputPattern).withZoneUTC
            Try(fmt.parseDateTime(y).withMillisOfDay(0)) match {
              case Success(i) if i == null => Right(null)
              case Success(i) =>
                val sdf = new SimpleDateFormat("dd/MM/yyyy")
                val date = sdf.format(i.getMillis)
                Right(date)
              case _ => Left(s"$fieldName $y not an String Date with pattern: $inputPattern")
            }
          case _ => Left(s"$fieldName  cannot be derived due to InsufficientData - $x ")
        }
      case _ => Left(s"$rawColumnName cannot be found")
    }

  }
}

case class Str2Double(override val fieldName: String, beginIndex: Int, endIndex: Int) extends TypeRule {

  override val targetType = Some(DoubleType)

  override def targetVal(row: Row, rawColumnName: String): Either[FieldError, Any] = {
    Try(row.getAs[String](rawColumnName)) match {
      case Success(x) if x == null => Right(null)
      case Success(x) =>
        Try(x.slice(beginIndex, endIndex).trim) match {
          case Success(y) if y.isEmpty => Right(null)
          case Success(y) => Try(y.toDouble) match {
            case Success(i) => Right(i)
            case _ => Left(s"$fieldName  $y not a String Decimal")
          }
          case _ => Left(s"$fieldName  cannot be derived due to InsufficientData - $x")
        }
      case _ => Left(s"$rawColumnName cannot be found")
    }
  }
}

case class Str2Str(override val fieldName: String, beginIndex: Int, endIndex: Int) extends TypeRule {

  override val targetType = Some(StringType)

  override def targetVal(row: Row, rawColumnName: String): Either[FieldError, Any] = {
    Try(row.getAs[String](rawColumnName)) match {
      case Success(x) if x == null => Right(null)
      case Success(x) =>
        Try(x.slice(beginIndex, endIndex).trim) match {
          case Success(y) if y.isEmpty => Right(null)
          case Success(y) => Right(y)
          case _ => Left(s"$fieldName  cannot be derived due to InsufficientData  $x")
        }
      case _ => Left(s"$rawColumnName cannot be found")
    }
  }
}