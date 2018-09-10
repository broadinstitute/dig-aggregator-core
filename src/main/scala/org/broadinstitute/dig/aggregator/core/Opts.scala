package org.broadinstitute.dig.aggregator.core

import java.io.File

import org.json4s._
import org.json4s.jackson.Serialization.read

import org.rogach.scallop.ScallopConf
import org.rogach.scallop.ScallopOption
import org.rogach.scallop.exceptions.ScallopException

import scala.io.Source

/**
 * Command line and configuration file argument parsing.
 */
class Opts(val appName: String, args: Array[String]) extends ScallopConf(args) {
  val configFile: ScallopOption[File] = opt("config", default = Some(new File("config.json")))

  /** Force Kafka consumption to process committed datasets and reset. */
  val reprocess: ScallopOption[Boolean] = opt("reprocess", required = false)

  /** Force Kafka consumption to process committed datasets and reset. */
  val reprocessAll: ScallopOption[Boolean] = opt("reprocess-all", required = false)
  
  /** Show version info and quit. */
  val version: ScallopOption[Boolean] = opt("version", required = false)

  // ensure the configuration file exists if provided
  validateFileExists(configFile)

  dependsOnAll(reprocessAll, List(reprocess))
  
  // parse arguments
  verify()

  /**
   * The app name - if any - to ignore datasets processed by
   */
  def ignoreProcessedBy: Option[String] = if(reprocessAll()) None else Option(appName)
  
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
  lazy val config: Config = Config.fromJson(configFile())

  /**
   * Outputs standard help from Scallop along with an additional message.
   */
  private def printHelp(message: String): Unit = {
    printHelp()
    println()
    println(message)
  }
}
