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

lazy val Paths = new {
  //`publish` will produce artifacts under this path
  val LocalRepo = "/humgen/diabetes/users/dig/aggregator/repo"
}

lazy val Resolvers = new {
  val LocalRepo = Resolver.file("localRepo", new File(Paths.LocalRepo))
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
    publishTo := Some(Resolvers.LocalRepo),
    libraryDependencies ++= (mainDeps ++ testDeps)
  )

//Make integration tests run serially.
parallelExecution in IntegrationTest := false

//Show full stack traces from unit and integration tests (F); display test run times (D)
testOptions in IntegrationTest += Tests.Argument("-oFD")
testOptions in Test += Tests.Argument("-oFD")

//Enables `buildInfoTask`, which bakes git version info into the LS jar.
enablePlugins(GitVersioning)

val buildInfoTask = taskKey[Seq[File]]("buildInfo")

buildInfoTask := {
  val dir = (resourceManaged in Compile).value
  val n = name.value
  val v = version.value
  val branch = git.gitCurrentBranch.value
  val lastCommit = git.gitHeadCommit.value
  val describedVersion = git.gitDescribedVersion.value
  val anyUncommittedChanges = git.gitUncommittedChanges.value

  val buildDate = java.time.Instant.now

  val file = dir / s"${n}-versionInfo.properties"

  val contents = s"name=${n}\nversion=${v}\nbranch=${branch}\nlastCommit=${lastCommit.getOrElse("")}\nuncommittedChanges=${anyUncommittedChanges}\ndescribedVersion=${describedVersion.getOrElse("")}\nbuildDate=${buildDate}\n"

  IO.write(file, contents)

  Seq(file)
}

(resourceGenerators in Compile) += buildInfoTask.taskValue
