name := "dig-aggregator-core"

version := "0.1.0"

scalaVersion := "2.12.6"

scalacOptions ++= Seq(
  "-feature",
  "-deprecation",
  "-Ypartial-unification",
  "-Ywarn-value-discard"
)

libraryDependencies ++= Seq(
  "ch.qos.logback" % "logback-classic" % "1.2.3",
  "co.fs2" %% "fs2-core" % "0.10.1",
  "com.amazonaws" % "aws-java-sdk" % "1.11.349",
  "com.chuusai" %% "shapeless" % "2.3.3",
  "com.outr" %% "scribe-slf4j" % "2.5.3",
  "org.json4s" %% "json4s-jackson" % "3.5.3",
  "org.rogach" %% "scallop" % "3.1.2",
  "org.typelevel" %% "cats-core" % "1.1.0",
  "org.typelevel" %% "cats-effect" % "1.0.0-RC2",
  "org.apache.hadoop" % "hadoop-client" % "1.2.1",
  "org.apache.kafka" %% "kafka" % "1.1.0"
)
