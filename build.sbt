import AssemblyKeys._
import sbt.ExclusionRule

name := "CassandraMigration"

version := "1.0"

scalaVersion := "2.10.3"

libraryDependencies += "org.scala-lang" % "scala-compiler" % "2.10.3"

libraryDependencies += "org.scala-lang" % "scala-library" % "2.10.3"

libraryDependencies += ("com.datastax.cassandra"  % "cassandra-driver-core" % "2.1.5")
  .exclude("org.mortbay.jetty", "servlet-api")

libraryDependencies += "com.typesafe" % "config" % "1.2.1"

libraryDependencies += "io.spray" %% "spray-json" % "1.3.1"

libraryDependencies += "org.specs2" %% "specs2-core" % "2.4.15" % "test"

libraryDependencies += "com.github.tototoshi" %% "scala-csv" % "1.2.2"

assemblySettings