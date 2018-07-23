package org.broadinstitute.dig.aggregator.core

import cats.effect._

import fs2._

import java.io.File
import java.util.Properties

import org.apache.kafka.clients.consumer._
import org.apache.kafka.common._

import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization.{read, writePretty}

import scala.collection.JavaConverters._

/**
 * Kafka JSON topic record consumer.
 */
final class Consumer[C <: BaseConfig](opts: Opts[C], topic: String) {
  val props: Properties = new Properties()

  // set all the properties
  props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, opts.config.kafka.brokerList)
  props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[serialization.StringDeserializer].getCanonicalName)
  props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[serialization.StringDeserializer].getCanonicalName)

  /**
   * The Kafka client used to receive variant JSON messages.
   */
  val client: KafkaConsumer[String, String] = new KafkaConsumer(props)

  /**
   * Get all the partitions for this topic.
   */
  val partitions: Seq[TopicPartition] = client.partitionsFor(topic).asScala.map {
    info => new TopicPartition(topic, info.partition)
  }

  /**
   * The current consumer state.
   */
  private var state: ConsumerState = opts.position match {
    case State.Continue  => State.load(new File(opts.config.kafka.consumers(topic)))
    case State.Beginning => State.fromBeginning(client, partitions)
    case State.End       => State.fromEnd(client, partitions)
  }

  // assign the partitions in the state
  client.assign(state.partitions.map(_.partition).asJava)

  // seek to the offset desired for each
  state.partitions.foreach {
    partitionState => client.seek(partitionState.partition, partitionState.offset)
  }

  /**
   * Create a Stream that will continuously read from Kafka and pass the
   * record batches to a process function.
   */
  def consume[A](process: ConsumerRecords[String, String] => IO[A]): IO[Unit] = {
    val fetch = IO {
      client.poll(Long.MaxValue)
    }

    // process the stream
    Stream.eval(fetch).repeat
      .evalMap(records => LiftIO[IO].liftIO(process(records)))
      .compile
      .drain
  }

  /**
   * Update the internal state with a ConsumerRecord.
   */
  def updateState(record: ConsumerRecord[String, String]): Unit = {
    state = state.withOffset(record.topic, record.partition, record.offset)
  }

  /**
   * Save the state of this consumer to disk.
   */
  def saveState(): IO[Unit] = IO {
    opts.config.kafka.consumers.get(topic).foreach {
      file => State.save(state, new File(file))
    }
  }
}
