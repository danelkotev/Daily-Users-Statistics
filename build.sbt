name := "daily-users-statistics"

version := "0.1"

scalaVersion := "2.12.10"

lazy val root = (project in file("."))
  .aggregate(
      calculator
  )

lazy val calculator = (project in file("statistics-calculator"))
  .enablePlugins(JavaAppPackaging)
  .settings(
    name := "statistics-calculator",
    resolvers += "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven",
    libraryDependencies ++= Dependencies.calculator
  )
