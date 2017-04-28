name := "spark-word-count"

version := "1.0"

scalaVersion := "2.10.4"
libraryDependencies += "org.apache.spark" % "spark-streaming_2.10" % "2.1.0"
libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.5.2"

libraryDependencies += "org.apache.spark" % "spark-sql_2.10" % "2.1.0"