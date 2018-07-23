package org.broadinstitute.dig.aggregator.core

import java.io.File

import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization.{read, writePretty}

import org.rogach.scallop._

import scala.io.Source

/**
 * Command line and configuration file argument parsing.
 */
final class Opts[C <: BaseConfig](args: Array[String])(implicit m: Manifest[C]) extends ScallopConf(args) {
  val configFile: ScallopOption[File] = opt[File]("config", default = Some(new File("config.json")))

  /** Force Kafka consumption from the beginning of time. */
  val fromBeginning: ScallopOption[Boolean] = opt[Boolean]("from-beginning")

  /** Force Kafka consumption to continue from the state file. */
  val continue: ScallopOption[Boolean] = opt[Boolean]("continue")

  // ensure the configuration file exists if provided
  validateFileExists(configFile)

  // both --continue and --from-beginning cannot be specified
  mutuallyExclusive(continue, fromBeginning)

  verify
  
  /* Private configuration settings. */
  lazy val config: C = configFile.toOption.map(Config.load[C]).get

  /** If --continue is not supplied, start from the beginning or end. */
  lazy val position: State.Position = {
    val from = continue.map(_ => State.Continue).toOption
    val beginning = fromBeginning.map(_ => State.Beginning).toOption

    Seq(from, beginning).flatten.headOption match {
      case Some(pos) => pos
      case None      => State.End
    }
  }
}
