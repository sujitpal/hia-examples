import sbt._
import sbt.Keys._

object ScaldingimpatientBuild extends Build {

  lazy val scaldingimpatient = Project(
    id = "scalding-impatient",
    base = file("."),
    settings = Project.defaultSettings ++ Seq(
      name := "scalding-impatient",
      organization := "com.mycompany",
      version := "0.1-SNAPSHOT",
      scalaVersion := "2.9.2"
      // add other settings here
    )
  )
}
