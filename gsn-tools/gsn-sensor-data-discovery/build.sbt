import com.typesafe.sbt.packager.Keys._

name := "gsn-sensor-data-discovery"

organization := "gsn"

version := "0.0.1-SNAPSHOT"

scalaVersion := "2.11.2"

crossPaths := false

scriptClasspath := Seq("*")

libraryDependencies ++= Seq(
  "org.apache.jena" % "apache-jena-libs" % "2.13.0" exclude("log4j","log4j") exclude("org.slf4j","slf4j-log4j12"),
  "com.typesafe" % "config" % "1.2.1",
  "commons-io" % "commons-io" % "2.4",
  "com.typesafe.play" %% "play-json" % "2.3.4"
)