package org.broadinstitute.dig.aggregator.core

import java.io.File

import org.broadinstitute.dig.aws.config.AwsConfig
import org.json4s.{DefaultFormats, Formats}
import org.json4s.jackson.Serialization.read

import scala.io.Source

/** Config is loaded from a JSON configuration file. They detail settings that are
  * used to make connections to databases and spin up machine clusters for processing.
  */
final case class Config(aws: AwsConfig)

/** Companion object. */
object Config {
  implicit val formats: Formats = DefaultFormats ++ AwsConfig.customSerializers

  /** Load the settings file and parse it. */
  def load(file: File): Config = {
    val source = Source.fromFile(file)

    try {
      read[Config](source.mkString)
    } finally {
      source.close
    }
  }
}
