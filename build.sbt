ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.12"

/*libraryDependencies += "au.com.bytecode" % "opencsv" % "2.4"*/

libraryDependencies += "com.opencsv" % "opencsv" % "5.9"
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.3.4"
// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.3.4"


lazy val root = (project in file("."))
  .settings(
    name := "GenerateCSVInScala"
  )
