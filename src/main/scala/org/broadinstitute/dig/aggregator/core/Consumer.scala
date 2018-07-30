package org.broadinstitute.dig.aggregator.core

import java.io.File
import java.util.Properties

import scala.collection.JavaConverters._

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization
import org.apache.kafka.common.TopicPartition

import cats.effect.IO
import fs2.Stream

/**
 * Kafka JSON topic record consumer.
 */
final class Consumer[C <: BaseConfig](opts: Opts[C], topic: String) {
  
  //NB: Use a helper method to build Properties, to minimize the amount of contructor logic interleaved in the 
  //class body.
  private val props: Properties = Props(
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> opts.config.kafka.brokerList,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[serialization.StringDeserializer].getCanonicalName,
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[serialization.StringDeserializer].getCanonicalName)

  /**
   * The Kafka client used to receive variant JSON messages.
   */
  private val client: KafkaConsumer[String, String] = new KafkaConsumer(props)

  /**
   * Get all the partitions for this topic.
   */
  //TODO: Should this be lazy?  Does it reach out to the broker when this class is instantiated?
  private lazy val partitions: Seq[TopicPartition] = client.partitionsFor(topic).asScala.map {
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
    Stream
        .eval(fetch)
        .repeat
        //Note that this previously used LiftIO[IO].liftIO(anIo), which does not appear to have any effect.
        //(ie, it turned an IO[A] into an IO[A].)
        .evalMap(process)
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
