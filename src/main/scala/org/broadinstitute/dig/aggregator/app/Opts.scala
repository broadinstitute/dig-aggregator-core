package org.broadinstitute.dig.aggregator.app

import java.io.File

import org.broadinstitute.dig.aggregator.core.Processor
import org.broadinstitute.dig.aggregator.core.config.{BaseConfig, Settings}
import org.rogach.scallop._
import org.rogach.scallop.exceptions.ScallopException

/** Command line and configuration file argument parsing. */
final class Opts(args: Seq[String]) extends ScallopConf(args) {

  /** JSON configuration file where settings are. */
  val configFile: ScallopOption[File] = opt("config", default = Some(new File("config.json")))

  /** Show version information. */
  val version: ScallopOption[Boolean] = opt("version")

  /** Show debug logging. */
  val debug: ScallopOption[Boolean] = opt("debug")

  /** The processor name is actually a pipeline name. */
  val pipeline: ScallopOption[Boolean] = opt("pipeline")

  /** Force processor to reprocess data it already has processed. */
  val reprocess: ScallopOption[Boolean] = opt("reprocess")

  /** Actually run the processor (as opposed to just showing work). */
  val yes: ScallopOption[Boolean] = opt("yes")

  /** Only process a single input with a given name (good for debugging). */
  val only: ScallopOption[String] = opt("only")

  /** Exclude processing that matches a given name. */
  val exclude: ScallopOption[String] = opt("exclude")

  /** Email errors to the given email address. */
  val emailOnFailure: ScallopOption[String] = opt("email-on-failure")

  /** Validate run results and delete (fix) runs from the database if necessary. */
  val verifyAndFix: ScallopOption[Boolean] = opt("verify-and-fix")

  /** Skip running, assume complete, and insert runs into the database. */
  val insertRuns: ScallopOption[Boolean] = opt("insert-runs")

  /** The processor (or pipeline if --pipeline specified) to run. */
  val processorName: ScallopOption[String] = trailArg(required = false)

  // test for options that don't go together
  mutuallyExclusive(version, processorName)
  mutuallyExclusive(only, pipeline)
  mutuallyExclusive(exclude, pipeline)
  mutuallyExclusive(verifyAndFix, reprocess)
  mutuallyExclusive(verifyAndFix, only)
  mutuallyExclusive(verifyAndFix, exclude)
  mutuallyExclusive(verifyAndFix, insertRuns)

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

  /** Private (not in version control) configuration settings. */
  lazy val config: BaseConfig = {
    configFile.toOption.map(Settings.load(_).config).get
  }

  /** The processor (or pipeline) to run. */
  def processor(): String = processorName.getOrElse("")

  /** The options used by processors. */
  def processorOpts: Processor.Opts = {
    Processor.Opts(reprocess(), insertRuns(), only.toOption, exclude.toOption)
  }

  /** Outputs standard help from Scallop along with an additional message. */
  private def printHelp(message: String): Unit = {
    printHelp()
    println()
    println(message)
  }
}
