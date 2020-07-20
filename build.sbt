resolvers += "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven"

name := "spark-pipeline"

version := "0.1"

scalaVersion := "2.12.10"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.0.0" % "provided"
libraryDependencies += "io.delta" %% "delta-core" % "0.7.0" % "provided"
libraryDependencies += "com.typesafe" % "config" % "1.3.1"
libraryDependencies += "mrpowers" % "spark-daria" % "0.35.2-s_2.12"

libraryDependencies += "MrPowers" % "spark-fast-tests" % "0.17.2-s_2.12" % "test"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test"

// test suite settings
fork in Test := true
javaOptions ++= Seq("-Xms2048M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled")
// Show runtime of tests
testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-oD")
