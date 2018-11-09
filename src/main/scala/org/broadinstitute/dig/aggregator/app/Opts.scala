package org.broadinstitute.dig.aggregator.app

import java.io.File

import org.broadinstitute.dig.aggregator.core.config.BaseConfig
import org.broadinstitute.dig.aggregator.core.processors.Processor

import org.json4s._
import org.json4s.jackson.Serialization.read

import org.rogach.scallop._
import org.rogach.scallop.exceptions.ScallopException

import scala.io.Source

/**
 * Command line and configuration file argument parsing.
 */
final class Opts(args: Seq[String]) extends ScallopConf(args) {

  /** JSON configuration file where settings are. */
  val configFile: ScallopOption[File] = opt("config", default = Some(new File("config.json")))

  /** Show version information. */
  val version: ScallopOption[Boolean] = opt("version")

  /** The processor name is actually a pipeline name. */
  val pipeline: ScallopOption[Boolean] = opt("pipeline")

  /** Force processor to reprocess data it already has processed. */
  val reprocess: ScallopOption[Boolean] = opt("reprocess")

  /** Actually run the processor (as opposed to just showing work). */
  val yes: ScallopOption[Boolean] = opt("yes")

  /** Only process a single input with a given name (good for debugging). */
  val only: ScallopOption[String] = opt("only")

  /** Email errors. */
  val emailOnFailure: ScallopOption[Boolean] = opt("email-on-failure")

  /** The processor (or pipeline if --pipeline specified) to run. */
  val processorName: ScallopOption[String] = trailArg(required = false)

  // run shouldn't be there if version is
  mutuallyExclusive(version, processorName)

  // parse the command line options
  verify

  /*
   * By default, Scallop will terminate the JVM on any ScallopExceptions, which
   * is very bad for testing. Provide new behavior, where ScallopExceptions
   * still get thrown, but don't terminate the JVM.
   */
  override def onError(e: Throwable): Unit = e match {
    case e @ ScallopException(msg) => printHelp(msg); throw e
    case ex                        => super.onError(ex)
  }

  /** Private (not in version control) configuration settings. */
  lazy val config: Opts.Config = {
    configFile.toOption.map(BaseConfig.load[Opts.Config]).get
  }

  /** The processor (or pipeline) to run. */
  def processor(): String = processorName.getOrElse("")

  /**
   * Outputs standard help from Scallop along with an additional message.
   */
  private def printHelp(message: String): Unit = {
    printHelp()
    println()
    println(message)
  }
}

object Opts {
  import org.broadinstitute.dig.aggregator.core.config._

  /**
   * A default implementation of BaseConfig.
   */
  final case class Config(
      aws: AWSConfig,
      mysql: MySQLConfig,
      neo4j: Neo4jConfig,
      sendgrid: SendgridConfig,
  ) extends BaseConfig
}
