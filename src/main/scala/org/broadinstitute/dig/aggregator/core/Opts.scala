package org.broadinstitute.dig.aggregator.core

import java.io.File

import org.rogach.scallop.ScallopConf
import org.rogach.scallop.ScallopOption
import org.rogach.scallop.exceptions.ScallopException

/**
 * Command line and configuration file argument parsing.
 */
class Opts[C <: BaseConfig](args: Array[String])(implicit m: Manifest[C])
    extends ScallopConf(args) {
  val configFile: ScallopOption[File] = opt("config", default = Some(new File("config.json")))

  /** Force Kafka consumption to process committed datasets and reset. */
  val reset: ScallopOption[Boolean] = opt("reset", required = false)

  /** Continue from where this application last left off. */
  val continue = reset.map(!_)

  /** Show version info and quit. */
  val version: ScallopOption[Boolean] = opt("version", required = false)

  // ensure the configuration file exists if provided
  validateFileExists(configFile)

  // parse arguments
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
  lazy val config: C = configFile.toOption.map(Config.load[C]).get

  /**
   * Outputs standard help from Scallop along with an additional message.
   */
  private def printHelp(message: String): Unit = {
    printHelp()
    println()
    println(message)
  }
}
