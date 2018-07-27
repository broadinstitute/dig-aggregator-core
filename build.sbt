lazy val Versions = new {
  val Scala = "2.12.6"
  val Fs2 = "0.10.1"
  val Aws = "1.11.349"
  val Shapeless = "2.3.3"
  val Json4s = "3.5.3"
  val Scallop = "3.1.2"
  val Cats = "1.1.0"
  val CatsEffect = "1.0.0-RC2"
  val Hadoop = "1.2.1"
  val Kafka = "1.1.0"
  val Slf4J = "1.7.25"
  val ScalaTest = "3.0.5"
}

lazy val Orgs = new {
  val DIG = "org.broadinstitute.dig"
}

lazy val scalacOpts = Seq(
  "-feature",
  "-deprecation",
  "-unchecked",
  "-Ypartial-unification",
  "-Ywarn-value-discard"
)

lazy val mainDeps = Seq(
  "org.slf4j" % "slf4j-api" % Versions.Slf4J,
  "co.fs2" %% "fs2-core" % Versions.Fs2,
  "com.amazonaws" % "aws-java-sdk" % Versions.Aws,
  "com.chuusai" %% "shapeless" % Versions.Shapeless,
  "org.json4s" %% "json4s-jackson" % Versions.Json4s,
  "org.rogach" %% "scallop" % Versions.Scallop,
  "org.typelevel" %% "cats-core" % Versions.Cats,
  "org.typelevel" %% "cats-effect" % Versions.CatsEffect,
  "org.apache.hadoop" % "hadoop-client" % Versions.Hadoop,
  "org.apache.kafka" %% "kafka" % Versions.Kafka
)

lazy val testDeps = Seq(
  "org.scalatest" %% "scalatest" % Versions.ScalaTest % "it,test"
)

lazy val root = (project in file("."))
  .configs(IntegrationTest)
  .settings(Defaults.itSettings : _*)
  .settings(
    name := "dig-aggregator-core",
    organization := Orgs.DIG,
    //NB: version set in version.sbt
    scalaVersion := Versions.Scala,
    scalacOptions ++= scalacOpts,
    libraryDependencies ++= (mainDeps ++ testDeps)
  )

//Make integration tests run serially.
parallelExecution in IntegrationTest := false

//Show full stack traces from unit and integration tests (F); display test run times (D)
testOptions in IntegrationTest += Tests.Argument("-oFD")
testOptions in Test += Tests.Argument("-oFD")
