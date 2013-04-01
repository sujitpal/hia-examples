import AssemblyKeys._

assemblySettings

name := "newsclip"

version := "1.0"

resolvers += "Conjars" at "http://conjars.org/repo"

libraryDependencies ++= Seq(
  "cascading" % "cascading-core" % "2.1.4",
  "cascading" % "cascading-local" % "2.1.4",
  "cascading" % "cascading-hadoop" % "2.1.4",
  "cascading" % "cascading-xml" % "2.1.4",
  "cascading" % "cascading-platform" % "2.1.4" % "test"
)
