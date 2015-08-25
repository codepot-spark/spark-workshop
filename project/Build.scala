import sbt.Keys._
import sbt._

object Build extends Build {

  lazy val root = Project(id = "root", base = file(".")).settings(
    name := "codepot-spark-starter",
    version := "1.0",
    scalaVersion := "2.11.7",
    scalacOptions += "-feature",
    scalacOptions in Test ++= Seq("-Yrangepos"),
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "1.4.1",
      "org.apache.spark" %% "spark-sql" % "1.4.1",
      "org.apache.spark" %% "spark-mllib" % "1.4.1",
      "org.specs2" %% "specs2-core" % "3.6.4" % "test"
    )

  )
}
