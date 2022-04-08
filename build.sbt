ThisBuild / version := "0.1.0"

ThisBuild / scalaVersion := "2.13.8"

lazy val root = (project in file("."))
  .settings(
    name := "Project2"
  )

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.2.1"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.2.1"
libraryDependencies += "org.apache.spark" %% "spark-hive" % "3.2.1"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "3.2.1"

libraryDependencies += "com.lihaoyi" %% "upickle" % "1.5.0"
libraryDependencies += "com.lihaoyi" %% "ujson" % "1.5.0"