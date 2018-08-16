package org.broadinstitute.dig.aggregator.core.config

/**
 * Kafka configuration settings.
 */
final case class Kafka(brokers: List[String]) {
  lazy val brokerList: String = brokers.mkString(",")
}
