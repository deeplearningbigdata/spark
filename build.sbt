name := "Pragramming Assessment"

version := "0.1"

scalaVersion := "2.11.8"

val sparkVersion = "2.2.0"

resolvers ++= Seq(
  "apache-snapshots" at "http://repository.apache.org/snapshots/"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core_2.11" % sparkVersion,
  "org.apache.spark" %% "spark-sql_2.11" % sparkVersion,
  "org.apache.spark" %% "spark-mllib_2.11" % sparkVersion,
  "org.apache.spark" %% "spark-streaming_2.11" % sparkVersion,
  "org.apache.spark" %% "spark-hive_2.11" % sparkVersion
)

