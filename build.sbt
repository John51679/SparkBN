name := "SparkBN"
version := "1.0"
scalaVersion := "2.12.12"

libraryDependencies ++= Seq(
    "org.apache.spark" % "spark-core_2.12" % "3.0.1",
    "org.apache.spark" % "spark-sql_2.12" % "3.0.1",
    "org.apache.spark" % "spark-streaming_2.12" % "3.0.1",
    "org.apache.spark" % "spark-mllib_2.12" % "3.0.1",
    "org.jmockit" % "jmockit" % "1.34" % "test"
)