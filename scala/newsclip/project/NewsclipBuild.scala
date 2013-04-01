import sbt._
import sbt.Keys._

object NewsclipBuild extends Build {

  lazy val newsclip = Project(
    id = "newsclip",
    base = file("."),
    settings = Project.defaultSettings ++ Seq(
      name := "newsclip",
      organization := "com.mycompany",
      version := "0.1-SNAPSHOT",
      scalaVersion := "2.9.2"
      // add other settings here
    )
  )
}
