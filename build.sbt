lazy val Versions = new {
  val Scala = "2.12.6"
  val LogbackClassic = "1.2.3"
  val Fs2 = "0.10.1"
  val Aws = "1.11.349"
  val Shapeless = "2.3.3"
  val ScribeSlf4J = "2.5.3"
  val Json4s = "3.5.3"
  val Scallop = "3.1.2"
  val Cats = "1.1.0"
  val CatsEffect = "1.0.0-RC2"
  val Hadoop = "1.2.1"
  val Kafka = "1.1.0"
}

lazy val Orgs = new {
  val DIG = "org.broadinstitute.dig"
}

organization := Orgs.DIG

name := "dig-aggregator-core"

//NB: version set in version.sbt

scalaVersion := Versions.Scala

scalacOptions ++= Seq(
  "-feature",
  "-deprecation",
  "-Ypartial-unification",
  "-Ywarn-value-discard"
)

libraryDependencies ++= Seq(
  "ch.qos.logback" % "logback-classic" % Versions.LogbackClassic,
  "co.fs2" %% "fs2-core" % Versions.Fs2,
  "com.amazonaws" % "aws-java-sdk" % Versions.Aws,
  "com.chuusai" %% "shapeless" % Versions.Shapeless,
  "com.outr" %% "scribe-slf4j" % Versions.ScribeSlf4J,
  "org.json4s" %% "json4s-jackson" % Versions.Json4s,
  "org.rogach" %% "scallop" % Versions.Scallop,
  "org.typelevel" %% "cats-core" % Versions.Cats,
  "org.typelevel" %% "cats-effect" % Versions.CatsEffect,
  "org.apache.hadoop" % "hadoop-client" % Versions.Hadoop,
  "org.apache.kafka" %% "kafka" % Versions.Kafka
)
