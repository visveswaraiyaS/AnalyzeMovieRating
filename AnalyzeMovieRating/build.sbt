ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.12"

lazy val root = (project in file("."))
  .settings(
    name := "AnalyzeMovieRating"
  )


val scalaTestVersion = "3.2.3"

libraryDependencies += "org.scalatest" %% "scalatest" % scalaTestVersion % "test"

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.2.0"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.2.0"

libraryDependencies += "org.apache.spark" %% "spark-mllib" % "3.2.0"

Test / parallelExecution := false