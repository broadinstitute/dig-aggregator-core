package org.broadinstitute.dig.aggregator.core.config

import java.io.File

import scala.io.Source

import org.broadinstitute.dig.aws.config.AWSConfig
import org.broadinstitute.dig.aws.config.Secrets
import org.broadinstitute.dig.aws.config.emr.EmrConfig
import org.json4s.DefaultFormats
import org.json4s.Formats
import org.json4s.jackson.Serialization.read

/** Settings are loaded from a JSON configuration file. They detail settings that are
  * used to make connections to databases and spin up machine clusters for processing.
  */
final case class Settings(aws: AWSConfig, mysqlSecretId: String, neo4jSecretId: String) {

  /** Download secrets and build the BaseConfig from the Settings. */
  lazy val config: BaseConfig = {
    val mysqlConfig = Secrets.get[MySQLConfig](mysqlSecretId)

    BaseConfig(aws, mysqlConfig.get)
  }
}

/** Companion object. */
object Settings {
  implicit val formats: Formats = DefaultFormats ++ EmrConfig.customSerializers

  /** Load the settings file and parse it. */
  def load(file: File): Settings = {
    read[Settings](Source.fromFile(file).mkString)
  }

  /** Load a settings file from a resource. */
  def loadResource(resource: String): Settings = {
    read[Settings](Source.fromResource(resource).mkString)
  }
}
