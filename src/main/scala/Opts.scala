package org.broadinstitute.dig.aggregator.core

import java.io.File

import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization.{read, writePretty}

import org.rogach.scallop._

import scala.io.Source

import scribe._

/**
 * Command line and configuration file argument parsing.
 */
class Opts[C <: Config](args: Array[String])(implicit m: Manifest[C]) extends ScallopConf(args) {
  val configFile = opt[File]("config")

  /** Force Kafka consumption from the beginning of time. */
  val fromBeginning = opt[Boolean]("from-beginning")

  /** Force Kafka consumption to continue from the state file. */
  val continue = opt[Boolean]("continue")

  // ensure the configuration file exists if provided
  validateFileExists(configFile)

  // both --continue and --from-beginning cannot be specified
  mutuallyExclusive(continue, fromBeginning)

  /* Private configuration settings. */
  lazy val config: C = {
    configFile.toOption.map(Config.load[C]).get
  }

  /** If --continue is not supplied, start from the beginning or end. */
  lazy val position = {
    val from = continue.map(_ => State.Continue).toOption
    val beginning = fromBeginning.map(_ => State.Beginning).toOption

    Seq(from, beginning).flatten.headOption match {
      case Some(pos) => pos
      case None      => State.End
    }
  }
}
