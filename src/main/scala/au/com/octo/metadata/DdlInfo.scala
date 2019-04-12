package au.com.octo.metadata

import au.com.octo.utils.exception.CsvGeneratorException

case class DdlInfo(column_name: String, length: Int, beginIndex: Int, endIndex: Int, dataType: String) {

  def asTypeRule: TypeRule = {
    toClassType(dataType) match {
      case Some(clazz) =>
        val constr = clazz.getConstructors()(0)
        constr.newInstance(column_name, new java.lang.Integer(beginIndex), new java.lang.Integer(endIndex)).asInstanceOf[TypeRule]
      case None => throw CsvGeneratorException(s"$dataType not supported")
    }
  }

  private[metadata] def toClassType(clazz: String): Option[Class[_]] =
    TypeRule.BY_SHORTNAME.get(clazz)
}
