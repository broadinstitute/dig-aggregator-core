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
class Opts(args: Seq[String]) extends ScallopConf(args) with Processor.Flags {

  /** JSON configuration file where settings are. */
  val configFile: ScallopOption[File] = opt("config", default = Some(new File("config.json")))

  /** Show version information. */
  val version: ScallopOption[Boolean] = opt("version")

  /** Name of the processor to run. */
  val processor: ScallopOption[String] = trailArg(required = false)

  // run shouldn't be there if version is
  mutuallyExclusive(version, processor)

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
  case class Config(
      kafka: KafkaConfig,
      aws: AWSConfig,
      mysql: MySQLConfig,
      neo4j: Neo4jConfig,
      sendgrid: SendgridConfig,
  ) extends BaseConfig
}
