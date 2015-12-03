organization := "io.ddf"

name := "ddf-test"

version := "0.0.1"

scalaVersion := "2.10.5"

scalacOptions := Seq("-unchecked", "-optimize", "-deprecation")

fork in Test := true

parallelExecution in ThisBuild := false

libraryDependencies := Seq("io.ddf" %% "ddf_core" % "1.5.0-SNAPSHOT",
  "org.scalatest" %% "scalatest" % "3.0.0-M7")
