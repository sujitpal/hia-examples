name := "scalding-impatient"

version := "1.0"

scalaVersion := "2.9.2"

// copied from scalding's build.sbt

resolvers += "Concurrent Maven Repo" at "http://conjars.org/repo"

libraryDependencies += "cascading" % "cascading-core" % "2.0.2"

libraryDependencies += "cascading" % "cascading-local" % "2.0.2"

libraryDependencies += "cascading" % "cascading-hadoop" % "2.0.2"

libraryDependencies += "cascading.kryo" % "cascading.kryo" % "0.4.4"

libraryDependencies += "com.twitter" % "meat-locker" % "0.3.0"

libraryDependencies += "com.twitter" % "maple" % "0.2.2"

libraryDependencies += "commons-lang" % "commons-lang" % "2.4"

// scalding (locally build)

libraryDependencies += "com.twitter" % "scalding_2.9.2" % "0.7.3"

libraryDependencies += "org.specs2" % "specs2_2.9.2" % "1.12.1"

// Invocation exception if we try to run the tests in parallel
parallelExecution in Test := false