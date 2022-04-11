ThisBuild / version := "0.1.0"
//original
//ThisBuild / scalaVersion := "2.13.8"
//new
ThisBuild / scalaVersion := "2.12.10"
lazy val root = (project in file("."))
  .settings(
    name := "Project2"
  )
//original
/*libraryDependencies += "org.apache.spark" %% "spark-core" % "3.2.1"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.2.1"
libraryDependencies += "org.apache.spark" %% "spark-hive" % "3.2.1"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "3.2.1"*/
//new
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.1.2"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.1.2"
libraryDependencies += "org.apache.spark" %% "spark-hive" % "3.1.2"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "3.1.2"

libraryDependencies += "com.lihaoyi" %% "upickle" % "1.5.0"
libraryDependencies += "com.lihaoyi" %% "ujson" % "1.5.0"