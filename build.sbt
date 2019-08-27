lazy val Versions = new {
  val Aws              = "1.11.349"
  val Cats             = "1.5.0"
  val CatsEffect       = "1.1.0"
  val Doobie           = "0.6.0"
  val Fs2              = "1.0.1"
  val H2               = "1.4.197"
  val Hadoop           = "1.2.1"
  val Janino           = "3.0.8"
  val Json4s           = "3.5.3"
  val LogbackClassic   = "1.2.3"
  val LogbackColorizer = "1.0.1"
  val MySQL            = "8.0.11"
  val Neo4j            = "1.7.0"
  val Scala            = "2.12.6"
  val ScalaLogging     = "3.7.2"
  val ScalaTest        = "3.0.5"
  val Scallop          = "3.1.2"
  val Sendgrid         = "4.2.1"
  val Shapeless        = "2.3.3"
  val Slf4J            = "1.7.25"
  val DigAws           = "0.1-SNAPSHOT"
}

lazy val Orgs = new {
  val DIG = "org.broadinstitute.dig"
}

mainClass := Some(s"${Orgs.DIG}.aggregator.app.Main")

lazy val scalacOpts = Seq(
  "-feature",
  "-deprecation",
  "-unchecked",
  "-Ypartial-unification",
  "-Ywarn-value-discard"
)

lazy val mainDeps = Seq(
  "co.fs2"                         %% "fs2-core"            % Versions.Fs2,
  "com.amazonaws"                  % "aws-java-sdk"         % Versions.Aws,
  "com.chuusai"                    %% "shapeless"           % Versions.Shapeless,
  "com.sendgrid"                   % "sendgrid-java"        % Versions.Sendgrid,
  "com.typesafe.scala-logging"     %% "scala-logging"       % Versions.ScalaLogging,
  "ch.qos.logback"                 % "logback-classic"      % Versions.LogbackClassic,
  "org.codehaus.janino"            % "janino"               % Versions.Janino,
  "org.json4s"                     %% "json4s-jackson"      % Versions.Json4s,
  "org.neo4j.driver"               % "neo4j-java-driver"    % Versions.Neo4j,
  "org.rogach"                     %% "scallop"             % Versions.Scallop,
  "org.tpolecat"                   %% "doobie-core"         % Versions.Doobie,
  "org.tpolecat"                   %% "doobie-hikari"       % Versions.Doobie,
  "org.tuxdude.logback.extensions" % "logback-colorizer"    % Versions.LogbackColorizer,
  "org.typelevel"                  %% "cats-core"           % Versions.Cats,
  "org.typelevel"                  %% "cats-effect"         % Versions.CatsEffect,
  "org.apache.hadoop"              % "hadoop-client"        % Versions.Hadoop,
  "mysql"                          % "mysql-connector-java" % Versions.MySQL,
  Orgs.DIG                         %% "dig-aws"             % Versions.DigAws
)

lazy val testDeps = Seq(
  "org.scalatest"  %% "scalatest" % Versions.ScalaTest % "it,test",
  "com.h2database" % "h2"         % Versions.H2        % "test"
)

lazy val root = (project in file("."))
  .configs(IntegrationTest)
  .settings(Defaults.itSettings: _*)
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

//Enables `buildInfoTask`, which bakes git version info into the LS jar.
enablePlugins(GitVersioning)

val buildInfoTask = taskKey[Seq[File]]("buildInfo")

buildInfoTask := {
  val dir                   = (resourceManaged in Compile).value
  val n                     = name.value
  val v                     = version.value
  val branch                = git.gitCurrentBranch.value
  val lastCommit            = git.gitHeadCommit.value
  val describedVersion      = git.gitDescribedVersion.value
  val anyUncommittedChanges = git.gitUncommittedChanges.value
  val remoteUrl             = (scmInfo in ThisBuild).value.map(_.browseUrl.toString)

  val buildDate = java.time.Instant.now

  val file = dir / "versionInfo.properties"

  val log = streams.value.log

  log.info(s"Writing version info to '$file'")

  val contents =
    s"""|name=${n}
        |version=${v}
        |branch=${branch}
        |lastCommit=${lastCommit.getOrElse("")}
        |uncommittedChanges=${anyUncommittedChanges}
        |describedVersion=${describedVersion.getOrElse("")}
        |buildDate=${buildDate}
        |remoteUrl=${remoteUrl.getOrElse("")}
        |""".stripMargin

  IO.write(file, contents)

  Seq(file)
}

(resourceGenerators in Compile) += buildInfoTask.taskValue

import ReleaseTransformations._

releaseProcess := Seq[ReleaseStep](
  checkSnapshotDependencies, // : ReleaseStep
  inquireVersions,           // : ReleaseStep
  runClean,                  // : ReleaseStep
  runTest,                   // : ReleaseStep
  setReleaseVersion,         // : ReleaseStep
  commitReleaseVersion,      // : ReleaseStep, performs the initial git checks
  tagRelease,                // : ReleaseStep
  // run 'publishLocal' instead of 'publish', since publishing to a repo on the Broad FS never resulted in
  // artifacts that could be resolved by other builds. :(
  // See: https://github.com/sbt/sbt-release#can-we-finally-customize-that-release-process-please
  //      https://stackoverflow.com/questions/44058275/add-docker-publish-step-to-sbt-release-process-with-new-tag
  //      https://github.com/sbt/sbt/issues/1917
  releaseStepCommand("publishLocal"),
  setNextVersion,    // : ReleaseStep
  commitNextVersion, // : ReleaseStep
  pushChanges        // : ReleaseStep, also checks that an upstream branch is properly configured
)
