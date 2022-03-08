ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.13.8"

val sparkVersion = "3.2.1"
val scalaTestVersion = "3.2.11"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion
)

lazy val root = (project in file("."))
  .settings(
    name := "BigDataSpark",
  )






