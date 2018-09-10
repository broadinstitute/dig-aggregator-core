package org.broadinstitute.dig.aggregator.core

import java.io.File
import scala.io.Source
import org.json4s.Formats
import org.json4s.DefaultFormats
import org.broadinstitute.dig.aggregator.core.config.DbConfig

/**
 * Configuration settings required by all aggregator applications.
 */
final case class Config(
    kafka: config.Kafka,
    aws: config.AWS,
    db: DbConfig,
    /*mysql: config.MySQL,*/
    neo4j: config.Neo4j,
    sendgrid: config.Sendgrid,
)

/**
 * Load and parse a configuration file.
 */

object Config {
  private implicit val formats: Formats = DefaultFormats
  
  import org.json4s.jackson.Serialization.read
  
  def fromJson(file: File): Config = fromJson(Source.fromFile(file).mkString)
  
  def fromJson(json: String): Config = read[Config](json)
}
