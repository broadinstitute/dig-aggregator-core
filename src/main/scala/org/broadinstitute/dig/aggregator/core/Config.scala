package org.broadinstitute.dig.aggregator.core

import cats.effect.IO

import com.typesafe.scalalogging.Logger

import com.zaxxer.hikari._

import doobie._
import doobie.hikari._

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
    extends BaseConfig {

  /**
   * Output to a logger all the information about this configuration.
   */
  def log(logger: Logger): Unit = {
    logger.info(s"App name=$app")
    logger.info(s"Kafka brokers=${kafka.brokerList}")
    logger.info(s"AWS key=${aws.key}")
    logger.info(s"AWS secret=${aws.secret}")
    logger.info(s"AWS EMR region=${aws.region}")
    logger.info(s"AWS S3 bucket=${aws.s3.bucket}")
    logger.info(s"MySQL url=${mysql.url}")
    logger.info(s"MysQL user=${mysql.user}")
    logger.info(s"MySQL password=${mysql.password}")
  }
}

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
final case class MySQLConfig(url: String,
                             driver: String,
                             schema: String,
                             user: String,
                             password: String) {

  /**
   * Query parameters to the connection string URL.
   */
  val qs = List("useCursorFetch" -> true, "useSSL" -> false)
    .map(p => s"${p._1}=${p._2}")
    .mkString("&")

  /**
   * Create a new connection to the database for running queries.
   *
   * A connection pool here isn't really all that big a deal because queries
   * are run serially while processing Kafka messages.
   */
  def newTransactor(): Transactor[IO] = {
    val connectionString = s"jdbc:mysql://$url/$schema?$qs"

    // create the connection
    Transactor.fromDriverManager[IO](
      driver,
      connectionString,
      user,
      password,
    )
  }
}
