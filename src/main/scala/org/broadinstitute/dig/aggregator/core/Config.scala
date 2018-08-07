package org.broadinstitute.dig.aggregator.core

import cats.effect.IO

import doobie._

import java.io.File

import scala.io.Source

import org.json4s.DefaultFormats
import org.json4s.Formats
import org.json4s.jackson.Serialization.read

/**
 * Companion object with methods for loading configuration files.
 */
object Config {
  implicit val formats: Formats = DefaultFormats

  /** Load and parse a configuration file. */
  def load[C <: BaseConfig](file: File)(implicit m: Manifest[C]): C = {
    read[C](Source.fromFile(file).mkString)
  }
}

/**
 * Base trait that all configuration files must adhere to.
 */
trait BaseConfig {
  val app: String
  val kafka: KafkaConfig
  val aws: AWSConfig
  val mysql: MySQLConfig
}

/**
 * Configuration options for Kafka and AWS.
 */
final case class Config(app: String, kafka: KafkaConfig, aws: AWSConfig, mysql: MySQLConfig)
    extends BaseConfig

/**
 * Kafka configuration settings.
 */
final case class KafkaConfig(brokers: List[String]) {
  lazy val brokerList: String = brokers.mkString(",")
}

/**
 * AWS configuration settings.
 */
final case class AWSConfig(key: String, secret: String, region: String, emr: EMR, s3: S3)

/**
 * Optional AWS EMR settings.
 */
final case class EMR(cluster: String)

/**
 * Optional AWS S3 settings.
 */
final case class S3(bucket: String)

/**
 * MysQL configuration settings.
 */
final case class MySQLConfig(url: String, driver: String, user: String, password: String) {

  /**
   * Create a connection to the database that can be used to execute queries.
   * The `schema` parameter is the database to open.
   */
  def createTransactor(schema: String): Transactor[IO] = {
    val connectionString = s"jdbc:mysql://$url/$schema?useCursorFetch=true"

    Transactor.fromDriverManager[IO](driver, connectionString, user, password)
  }
}
