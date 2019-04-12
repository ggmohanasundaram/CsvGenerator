package au.com.octo.utils.exception

case class CsvGeneratorException(message: String = "") extends Exception(message)
