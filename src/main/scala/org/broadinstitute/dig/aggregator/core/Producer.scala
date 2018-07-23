package org.broadinstitute.dig.aggregator.core

import cats.effect._

import java.util.Properties

import org.apache.kafka.clients.producer._
import org.apache.kafka.common._

import scala.concurrent._

/**
 * Kafka JSON topic record producer.
 */
class Producer[C <: BaseConfig](opts: Opts[C], topic: String)(implicit ec: ExecutionContext) {
  val props = new Properties()

  // set all the properties
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, opts.config.kafka.brokerList)
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[serialization.StringSerializer].getCanonicalName)
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[serialization.StringSerializer].getCanonicalName)
  props.put(ProducerConfig.ACKS_CONFIG, "1")
  props.put(ProducerConfig.RETRIES_CONFIG, "3")
  props.put(ProducerConfig.LINGER_MS_CONFIG, "5")

  /**
   * The Kafka producer client to sent variant messages to.
   */
  val client = new KafkaProducer[String, String](props)

  /**
   * Send a JSON message to the Kafka queue to a given topic.
   */
  def send(key: String, value: String): IO[RecordMetadata] = {
    val future = Future {
      client.send(new ProducerRecord[String, String](topic, key, value)).get
    }

    // submit it to kafka
    IO.fromFuture(IO(future))
  }
}
