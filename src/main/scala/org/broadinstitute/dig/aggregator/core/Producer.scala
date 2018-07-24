package org.broadinstitute.dig.aggregator.core

import cats.effect._

import java.util.Properties

import org.apache.kafka.clients.producer._
import org.apache.kafka.common._

import scala.concurrent._

/**
 * Kafka JSON topic record producer.
 */
final class Producer[C <: BaseConfig](opts: Opts[C], topic: String)(implicit ec: ExecutionContext) {
  //NB: Use a helper method to build Properties, to minimize the amount of contructor logic interleaved in the 
  //class body.
  private val props: Properties = Props(
    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> opts.config.kafka.brokerList,
    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG -> classOf[serialization.StringSerializer].getCanonicalName,
    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> classOf[serialization.StringSerializer].getCanonicalName,
    ProducerConfig.ACKS_CONFIG -> "1",
    ProducerConfig.RETRIES_CONFIG -> "3",
    ProducerConfig.LINGER_MS_CONFIG -> "5")

  /**
   * The Kafka producer client to sent variant messages to.
   */
  private val client: KafkaProducer[String, String] = new KafkaProducer(props)

  /**
   * Send a JSON message to the Kafka queue to a given topic.
   */
  def send(key: String, value: String): IO[RecordMetadata] = {
    //Note that previously, the code made a Future and then wrapped it in an IO.  Futures start running immediately,
    //so this seemed like a problem, or at least, something very unexpected given that the result is an IO.
    val futureIo = IO {
      Future {
        client.send(new ProducerRecord[String, String](topic, key, value)).get
      }
    }

    // submit it to kafka
    IO.fromFuture(futureIo)
  }
}
