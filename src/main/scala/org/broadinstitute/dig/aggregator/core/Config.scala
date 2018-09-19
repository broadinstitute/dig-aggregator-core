package org.broadinstitute.dig.aggregator.core

import java.io.File
import scala.io.Source
import org.json4s.Formats
import org.json4s.DefaultFormats
import org.broadinstitute.dig.aggregator.core.config.DbConfig
import org.broadinstitute.dig.aggregator.core.config.Dbs
import org.broadinstitute.dig.aggregator.core.config.DbType

/**
 * Configuration settings required by all aggregator applications.
 */
final case class Config(
    kafka: config.Kafka,
    aws: config.AWS,
    dbs: config.Dbs,
    h2: config.DbConfig,
    mysql: config.DbConfig,
    neo4j: config.Neo4j,
    sendgrid: config.Sendgrid,
) {
  
  def readDb: DbConfig = dbForType(dbs.read)
  
  def writeDb: DbConfig = dbForType(dbs.write)
  
  private def dbForType(dbType: DbType): DbConfig = dbType match {
    case DbType.H2 => h2
    case DbType.MySql => mysql
  }
  
  def newTransactors: Xas = Xas(read = readDb.newTransactor, write = writeDb.newTransactor) 
}

/**
 * Load and parse a configuration file.
 */

object Config {
  private implicit val formats: Formats = DefaultFormats
  
  import org.json4s.jackson.Serialization.read
  
  def fromJson(file: File): Config = fromJson(Source.fromFile(file).mkString)
  
  def fromJson(json: String): Config = read[Config](json)
}
