package org.broadinstitute.dig.aggregator.core

import cats.effect.IO

import doobie._

import fs2.Stream

import java.io.File
import java.util.Properties

import org.apache.kafka.clients.consumer._
import org.apache.kafka.common._

import scala.collection.JavaConverters._
import scala.io.StdIn

/**
 * Kafka JSON topic record consumer.
 */
final class Consumer(config: BaseConfig, topic: String) {

  /**
   * Helper types since the template parameters are constant.
   */
  type Record = ConsumerRecord[String, String]
  type Records = ConsumerRecords[String, String]

  /**
   * MySQL database connection for the consumer state.
   */
  private val xa: Transactor[IO] = config.mysql.createTransactor("kafka")

  /**
   * Kafka connection properties.
   */
  private val props: Properties = Props(
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> config.kafka.brokerList,
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[serialization.StringDeserializer].getCanonicalName,
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[serialization.StringDeserializer].getCanonicalName
  )

  /**
   * The Kafka client used to receive variant JSON messages.
   */
  private val client: KafkaConsumer[String, String] = new KafkaConsumer(props)

  /**
   * Get all the partitions for this topic.
   */
  private val partitions: Seq[Int] = client.partitionsFor(topic).asScala.map(_.partition)

  /**
   * The current consumer state.
   */
  private var state: State = ???

  /**
   * Seek to the latest offsets for this topic.
   */
  private[this] def resetState(): IO[Unit] = {
    val topicPartitions = partitions.map(new TopicPartition(topic, _))
    val beginningOffsets = client.beginningOffsets(topicPartitions.asJava)

    // convert the partition offsets to a map of partition -> offset
    val offsets = beginningOffsets.asScala.map {
      case (topicPartition, offset) => topicPartition.partition -> offset.toLong
    }

    // BIG RED LETTERS FOR THE USER!
    println("WARNING! The consumer state is being reset because either")
    println("         reset flag was passed on the command line or the")
    println("         commits database doesn't contain any partition")
    println("         offsets for this app/topic pair.")
    println()
    println("         If this is the desired course of action, answer Y")
    println("         at the prompt; any other response will terminate")
    println("         the program before any damage is done.")
    println()

    // terminate the entire application if the user doesn't answer "Y"
    if (StdIn.readLine("[y/N]: ") != "Y") {
      System.exit(0)
    }

    // create the state from a merge of the beginning offsets and the latest
    State.latest(xa, topic, offsets.toMap).map { offsets =>
      state = new State(config.app, topic, offsets)
    }
  }

  /**
   * Seek to the offsets specified in the consumer state of the database. If
   * the database doesn't actually contain any partition offsets for this app
   * and topic, then default to using reset state.
   */
  private[this] def loadState(): IO[Unit] = {
    State.load(xa, config.app, topic).flatMap {
      case Some(s) => IO { state = s }
      case None    => resetState()
    }
  }

  /**
   * Before the consumer can start consuming records it must seek to the
   * correct offset for each partition, which initializes the state.
   */
  def assignPartitions(reset: Boolean): IO[Unit] = {
    val getState = if (reset) resetState() else loadState()

    // load the state, then assign partitions and seek to offsets
    for (_ <- getState) yield {
      client.assign(state.offsets.keys.map(new TopicPartition(topic, _)).toList.asJava)

      for ((partition, offset) <- state.offsets) {
        client.seek(new TopicPartition(topic, partition), offset)
      }
    }
  }

  /**
   * Create a Stream that will continuously read from Kafka and pass the
   * record batches to a process function.
   */
  def consume[A](process: Records => IO[A]): IO[Unit] = {
    val fetch = IO {
      client.poll(Long.MaxValue)
    }

    // process the stream
    Stream
      .eval(fetch)
      .repeat
      .evalMap(process)
      .compile
      .drain
  }

  /**
   * Update the internal state with a ConsumerRecord.
   */
  def updateState(record: Record): Unit = {
    require(record.topic.equals(topic), s"Record topic doesn't match $topic!")

    // update the offset map in the state
    state.update(record)
  }

  /**
   * Save the state of this consumer to the database.
   */
  def saveState(): IO[Boolean] = {
    state.save(xa)
  }
}
