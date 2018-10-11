package org.broadinstitute.dig.aggregator.core.config

/**
 * Kafka configuration settings.
 */
final case class KafkaConfig(brokers: List[String]) {
  lazy val brokerList: String = brokers.mkString(",")
}
