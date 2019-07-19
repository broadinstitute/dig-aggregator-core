package org.broadinstitute.dig.aggregator.core.config

import java.io.File

import org.json4s.jackson.Serialization.read
import org.json4s.{DefaultFormats, Formats}

import scala.io.Source

/** Settings are loaded from a JSON configuration file. They detail settings that are
  * used to
  */
final case class Settings(aws: AWSConfig, mysql: String, neo4j: String) {

  /** Download secrets and build the BaseConfig from the Settings. */
  lazy val config: BaseConfig = {
    val mysqlConfig = Secrets.get[MySQLConfig](mysql)
    val neo4jConfig = Secrets.get[Neo4jConfig](neo4j)

    BaseConfig(aws, mysqlConfig.get, neo4jConfig.get)
  }
}

/** Companion object. */
object Settings {
  implicit val formats: Formats = DefaultFormats ++ emr.EmrConfig.customSerializers

  /** Load the settings file and parse it. */
  def load(file: File): Settings = {
    read[Settings](Source.fromFile(file).mkString)
  }
}
