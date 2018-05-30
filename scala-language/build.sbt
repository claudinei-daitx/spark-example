name := "agile-big-data-scala"

version := "0.1"

scalaVersion := "2.11.12"


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.3.0",
  "org.apache.spark" %% "spark-sql" % "2.3.0",
  "org.apache.spark" %% "spark-graphx" % "2.3.0",
  "org.apache.avro"  %  "avro"  %  "1.8.2",
  "com.databricks" %% "spark-avro" % "4.0.0",
  "org.scalatest" %% "scalatest" % "3.0.5" % "test"
)