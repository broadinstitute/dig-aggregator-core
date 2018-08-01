package org.broadinstitute.dig.aggregator.core

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
  val kafka: KafkaConfig
  val aws: AWSConfig
}

/**
 * Configuration options for Kafka and AWS.
 */
final case class Config(kafka: KafkaConfig, aws: AWSConfig) extends BaseConfig

/**
 * Kafka configuration settings.
 */
final case class KafkaConfig(brokers: List[String], consumers: Map[String, String]) {
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
