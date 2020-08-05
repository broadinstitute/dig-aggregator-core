lazy val Versions = new {
  val Atto             = "0.7.0"
  val H2               = "1.4.200"
  val Hikari           = "3.4.5"
  val Janino           = "3.1.2"
  val Json4s           = "3.6.8"
  val LogbackClassic   = "1.2.3"
  val LogbackColorizer = "1.0.1"
  val MySQL            = "8.0.17"
  val Quill            = "3.5.1"
  val Scala            = "2.13.2"
  val ScalaLogging     = "3.9.2"
  val ScalaTest        = "3.1.2"
  val Scallop          = "3.5.0"
  val DigAws           = "0.3.0-SNAPSHOT"
}

lazy val Orgs = new {
  val DIG = "org.broadinstitute.dig"
}

mainClass := None

lazy val scalacOpts = Seq(
  "-feature",
  "-deprecation",
  "-unchecked",
  "-Ywarn-value-discard",
  "-language:existentials"
)

lazy val mainDeps = Seq(
  "com.h2database"                 % "h2"                        % Versions.H2,
  "com.typesafe.scala-logging"     %% "scala-logging"            % Versions.ScalaLogging,
  "ch.qos.logback"                 % "logback-classic"           % Versions.LogbackClassic,
  "org.codehaus.janino"            % "janino"                    % Versions.Janino,
  "org.json4s"                     %% "json4s-jackson"           % Versions.Json4s,
  "io.getquill"                    %% "quill-jdbc"               % Versions.Quill,
  "com.zaxxer"                     % "HikariCP"                  % Versions.Hikari,
  "org.rogach"                     %% "scallop"                  % Versions.Scallop,
  "org.tpolecat"                   %% "atto-core"                % Versions.Atto,
  "org.tuxdude.logback.extensions" % "logback-colorizer"         % Versions.LogbackColorizer,
  "mysql"                          % "mysql-connector-java"      % Versions.MySQL,
  Orgs.DIG                         %% "dig-aws"                  % Versions.DigAws
)

lazy val testDeps = Seq(
  "org.scalatest"  %% "scalatest" % Versions.ScalaTest % "it,test"
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

// make integration tests run serially.
parallelExecution in IntegrationTest := false

// don't run scaladoc when publishing locally
publishArtifact in (Compile, packageDoc) := false

// show full stack traces from unit and integration tests (F); display test run times (D)
testOptions in IntegrationTest += Tests.Argument("-oFD")
testOptions in Test += Tests.Argument("-oFD")

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
