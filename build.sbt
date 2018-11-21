name := "advanced-analytics-with-spark"

version := "0.1"

scalaVersion := "2.12.7"

val SPARK_VERSION = "2.4.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % SPARK_VERSION,
  "org.apache.spark" %% "spark-mllib" % SPARK_VERSION,
  "com.github.fommil.netlib" % "all" % "1.1.2" pomOnly()
)