package org.broadinstitute.dig.aggregator.core

import cats._
import cats.effect._
import cats.effect.concurrent._
import cats.implicits._

import doobie._

import fs2.Stream

import java.io.File
import java.util.Properties

import org.apache.kafka.clients.consumer._
import org.apache.kafka.common._

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.io.StdIn

/**
 * Kafka JSON topic record consumer.
 */
final class Consumer(config: BaseConfig, topic: String) {

  /**
   *Create a connection to the database for writing state.
   */
  val xa = config.mysql.newTransactor()

  /**
   * Helper types since the template parameters are constant.
   */
  type Record  = ConsumerRecord[String, String]
  type Records = ConsumerRecords[String, String]

  /**
   * Kafka connection properties.
   */
  private val props: Properties = Props(
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG        -> config.kafka.brokerList,
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG   -> classOf[serialization.StringDeserializer].getCanonicalName,
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
   * Get the earliest offsets for each partition in this topic. Then load
   * the latest offsets for each of those partitions from the commits table
   * and merge the results into a new state.
   */
  private[this] def resetState(): IO[State] = {
    val topicPartitions  = partitions.map(new TopicPartition(topic, _))
    val beginningOffsets = client.beginningOffsets(topicPartitions.asJava)

    // convert the partition offsets to a map of partition -> offset
    val offsets = beginningOffsets.asScala.map {
      case (topicPartition, offset) => topicPartition.partition -> offset.toLong
    }

    // BIG RED LETTERS FOR THE USER!
    println("WARNING! The consumer state is being reset because either reset")
    println("         flag was passed on the command line or the commits")
    println("         database doesn't contain any partition offsets for this")
    println("         application + topic.")
    println()
    println("         If this is the desired course of action, answer 'Y' at")
    println("         the prompt; any other response will exit the program")
    println("         before any damage is done.")
    println()

    // terminate the entire application if the user doesn't answer "Y"
    if (StdIn.readLine("[y/N]: ").equalsIgnoreCase("y")) {
      System.exit(0)
    }

    // create the state from a merge of the beginning offsets and the latest
    State.reset(xa, config.app, topic, offsets.toMap).map {
      new State(config.app, topic, _)
    }
  }

  /**
   * Load the set of partitions and offsets this application has saved to the
   * database for this topic. If the database doesn't actually contain any
   * partition offsets for this app + topic, then reset the state.
   */
  private[this] def loadState(): IO[State] = {
    State.load(xa, config.app, topic).flatMap {
      case Some(s) => IO(s)
      case None    => resetState()
    }
  }

  /**
   * Before the consumer can start consuming records it must seek to the
   * correct offset for each partition, which initializes the state. If
   * the reset flag was passed on the command line then `reset` is true
   * and forced, otherwise it attempts to load the last state from the
   * database with resetState used as a fallback.
   */
  private[this] def assignPartitions(reset: Boolean): IO[State] = {
    val getState = if (reset) resetState() else loadState()

    // load or reset the state, then assign the partitions
    for (state <- getState) yield {
      client.assign(state.offsets.keys.map(new TopicPartition(topic, _)).toList.asJava)

      // advance the partitions to the correct offsets
      for ((partition, offset) <- state.offsets) {
        client.seek(new TopicPartition(topic, partition), offset)
      }

      // initial state
      state
    }
  }

  /**
   * Constantly grab the last set of records processed and try to update -
   * and save to the database - a new state. If no new records are there,
   * just wait a little while before checking again.
   */
  private[this] def updateState(state: State, ref: Ref[IO, Option[Records]]): IO[Unit] = {
    val wait = IO.sleep(10.seconds)

    /*
     * Wait a bit, then take - and return - whatever is in the ref (`_`) and
     * replace it with `None`. If there were records in the ref, update the
     * state and write it to the database. Then recurse.
     */
    wait >> ref.modify(None -> _).flatMap {
      case Some(records) => (state ++ records).save(xa).flatMap(updateState(_, ref))
      case None          => updateState(state, ref)
    }
  }

  /**
   * Create a Stream that will continuously read from Kafka and pass the
   * records to a process function.
   */
  def consume[A](reset: Boolean)(process: Records => IO[A]): IO[Unit] = {
    val fetch = IO {
      client.poll(Long.MaxValue)
    }

    for {
      ref   <- Ref[IO].of[Option[Records]](None)
      state <- assignPartitions(reset)

      // create tasks to save the state and another to process the stream
      saveTask = updateState(state, ref)
      streamTask = Stream.eval(fetch)
        .repeat
        .evalMap(rs => process(rs) >> ref.set(Some(rs)))
        .compile
        .drain

      // run each task asynchronously in a fiber
      streamFiber <- streamTask.start
      saveFiber   <- saveTask.start

      // wait for them to both complete (which will never happen)
      _ <- streamFiber.join
      _ <- saveFiber.join
    } yield ()
  }
}
