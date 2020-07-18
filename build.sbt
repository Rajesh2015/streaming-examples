name := "streaming-examples"

version := "0.1"

scalaVersion := "2.11.8"

// Spark Core, Spark SQL and Spark Streaming dependencies
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.2.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.2.0"
libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.2.0"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.2.0" % "provided"

//configuration file dependency
libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.7.2"
libraryDependencies += "com.typesafe" % "config" % "1.2.1"

// Reading data from s3 SBT dependency
libraryDependencies += "org.apache.hadoop" % "hadoop-aws" % "2.7.4"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs@_*) => MergeStrategy.discard
  case x => MergeStrategy.first
}