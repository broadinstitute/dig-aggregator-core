package org.broadinstitute.dig.aggregator.core

import java.io.File

import org.rogach.scallop._
import org.rogach.scallop.exceptions.ScallopException

/** Command line and configuration file argument parsing. */
final class Opts(args: Seq[String]) extends ScallopConf(args) {
  import Implicits.stringToGlob

  /** Dot-env configuration file where settings are. */
  val configFile: ScallopOption[File] = opt("config", default = Some(new File("config.json")))

  /** Show version information. */
  val version: ScallopOption[Boolean] = opt("version")

  /** Redirect stage output to a test folder in S3. */
  val test: ScallopOption[Boolean] = opt("test")

  /** Provide a specific stage name in the method to run. */
  val stage: ScallopOption[String] = opt("stage")

  /** Force processor to reprocess data it already has processed. */
  val reprocess: ScallopOption[Boolean] = opt("reprocess")

  /** Actually run the processor (as opposed to just showing work). */
  val yes: ScallopOption[Boolean] = opt("yes")

  /** Only process a single input with a given name (good for debugging). */
  val only: ScallopOption[String] = opt("only")

  /** Exclude processing that matches a given name. */
  val exclude: ScallopOption[String] = opt("exclude")

  /** Show the inputs used to process a given output. */
  val showInputs: ScallopOption[Boolean] = opt("show-inputs")

  /** Skip running, assume complete, and insert runs into the database. */
  val insertRuns: ScallopOption[Boolean] = opt("insert-runs")

  /** Run the processor, but do NOT insert runs when done. */
  val noInsertRuns: ScallopOption[Boolean] = opt("no-insert-runs")

  // test for options that don't go together
  mutuallyExclusive(insertRuns, noInsertRuns)

  // parse the command line options
  verify

  /** By default, Scallop will terminate the JVM on any ScallopExceptions, which
    * is very bad for testing. Provide new behavior, where ScallopExceptions
    * still get thrown, but don't terminate the JVM.
    */
  override def onError(e: Throwable): Unit = e match {
    case e @ ScallopException(msg) => printHelp(msg); throw e
    case ex                        => super.onError(ex)
  }

  /** Just the opposite of --yes. */
  lazy val dryRun: ScallopOption[Boolean] = yes.map(!_)

  /** Private (not in version control) configuration settings. */
  lazy val config: Config = {
    configFile.toOption.map(Config.load).get
  }

  /** Returns a glob for the only option. */
  lazy val onlyGlobs: Option[Seq[Glob]] = {
    only.map(_.split(",").filter(_.nonEmpty).map(stringToGlob).toSeq).toOption
  }

  /** Returns a glob for the exclude option. */
  lazy val excludeGlobs: Option[Seq[Glob]] = {
    exclude.map(_.split(",").filter(_.nonEmpty).map(stringToGlob).toSeq).toOption
  }

  /** Outputs standard help from Scallop along with an additional message. */
  private def printHelp(message: String): Unit = {
    printHelp()
    println()
    println(message)
  }
}
