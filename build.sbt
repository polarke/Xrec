name := "xrec"

version := "0.1"

scalaVersion := "2.11.5"
// scalaVersion := "2.10.4"

scalacOptions ++= Seq("-feature")

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.3.1"
//libraryDependencies += "org.apache.spark" %% "spark-core" % "1.3.0"

libraryDependencies += "org.scalatest" % "scalatest_2.11" % "2.2.4" % "test"

