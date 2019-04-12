name := "CsvGenerator"
version := "0.1"

val sparkVersion = sys.env.getOrElse("SPARK_VERSION", "2.4.0")
parallelExecution in Test := false
organization := "au.com.octo"
scalaVersion := "2.11.11"

libraryDependencies ++= Seq( "com.typesafe" % "config" % "1.3.1",
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion ,
  "org.specs2" %% "specs2-core" % "4.0.1" % Test,
  "org.specs2" %% "specs2-matcher-extra" % "4.0.1"% Test,
  "org.specs2" %% "specs2-mock" % "4.0.1" % Test)