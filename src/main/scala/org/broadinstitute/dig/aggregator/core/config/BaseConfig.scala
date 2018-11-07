package org.broadinstitute.dig.aggregator.core.config

import java.io.File

import org.json4s._
import org.json4s.jackson.Serialization.read

import scala.io.Source

/**
 * Private configuration settings required by pretty much everything.
 */
trait BaseConfig {
  val aws: AWSConfig
  val mysql: MySQLConfig
  val neo4j: Neo4jConfig
  val sendgrid: SendgridConfig
}

/**
 * Companion object for loading configuration files.
 */
object BaseConfig {
  implicit val formats: Formats = DefaultFormats

  /**
   * Load and parse a configuration file.
   */
  def load[A <: BaseConfig](file: File)(implicit m: Manifest[A]): A = {
    read[A](Source.fromFile(file).mkString)
  }
}
