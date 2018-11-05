package org.broadinstitute.dig.aggregator.core

import cats.effect.IO

import java.util.Properties

import org.apache.kafka.clients.producer._
import org.apache.kafka.common.serialization

import org.broadinstitute.dig.aggregator.core.config.KafkaConfig

/**
 * Kafka JSON topic record producer.
 */
final class Producer(config: KafkaConfig, topic: String) {

  import Producer.Record

  /**
   * Kafka connection properties.
   */
  private val props: Properties = utils.Props(
    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG      -> config.brokerList,
    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG   -> classOf[serialization.StringSerializer].getCanonicalName,
    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> classOf[serialization.StringSerializer].getCanonicalName,
    ProducerConfig.ACKS_CONFIG                   -> "1",
    ProducerConfig.RETRIES_CONFIG                -> "3",
    ProducerConfig.LINGER_MS_CONFIG              -> "5"
  )

  /**
   * The Kafka producer client to sent variant messages to.
   */
  private val client: KafkaProducer[String, String] = {
    Thread.currentThread.setContextClassLoader(null)

    /*
     * This is a hack so that Kafka can find the serializer classes!
     */

    new KafkaProducer(props)
  }

  /**
   * Send a JSON message to the Kafka queue to a given topic.
   */
  def send(key: String, value: String): IO[RecordMetadata] = {
    IO {
      client.send(new Record(topic, key, value)).get
    }
  }
}

object Producer {

  /**
   * Helper type alias since template parameters are fixed.
   */
  type Record = ProducerRecord[String, String]
}
