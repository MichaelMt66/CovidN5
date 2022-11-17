//ThisBuild / version := "0.1.0-SNAPSHOT"
//
//ThisBuild / scalaVersion := "2.12.8"
//
//lazy val root = (project in file("."))
//  .settings(
//    name := "N5-challenge"
//  )


name := "covid-stuff"
version := "0.1"
scalaVersion := "2.11.8"
organization := "MyHome"

resolvers ++= Seq(
  "jitpack.io" at "https://jitpack.io",
  "Artima Maven Repository" at "http://repo.artima.com/releases"
)

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.0"
libraryDependencies += "com.typesafe" % "config" % "1.3.3"
libraryDependencies += "org.xerial.snappy" % "snappy-java" % "1.1.8.4"

