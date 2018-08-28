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
import com.typesafe.scalalogging.LazyLogging

/**
 * Kafka JSON topic record consumer.
 */
final class Consumer[A](opts: Opts, topic: String)(fromRecord: Consumer.Record => A)
    extends LazyLogging {

  /**
   * Create a connection to the database for writing state.
   */
  val xa = opts.config.mysql.newTransactor()

  /**
   * Kafka connection properties.
   */
  private val props: Properties = Props(
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG        -> opts.config.kafka.brokerList,
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
  private def resetState(): IO[State] = {
    val topicPartitions  = partitions.map(new TopicPartition(topic, _))
    val beginningOffsets = client.beginningOffsets(topicPartitions.asJava)

    // convert the partition offsets to a map of partition -> offset
    val offsets = beginningOffsets.asScala.map {
      case (topicPartition, offset) => topicPartition.partition -> offset.toLong
    }

    /*
     * BIG RED LETTERS FOR THE USER!
     */
    logger.error("WARNING! The consumer state is being reset because either reset")
    logger.error("         flag was passed on the command line or the commits")
    logger.error("         database doesn't contain any partition offsets for this")
    logger.error("         application + topic.")
    logger.error("")
    logger.error("         If this is the desired course of action, answer 'Y' at")
    logger.error("         the prompt; any other response will exit the program")
    logger.error("         before any damage is done.")
    logger.error("")

    // terminate the entire application if the user doesn't answer "Y"
    if (!StdIn.readLine("[y/N]: ").equalsIgnoreCase("y")) {
      IO.raiseError(new Exception("state reset canceled"))
    } else {
      State.reset(xa, opts.appName, topic, offsets.toMap).map { offsets =>
        State(opts.appName, topic, offsets)
      }
    }
  }

  /**
   * Load the set of partitions and offsets this application has saved to the
   * database for this topic. If the database doesn't actually contain any
   * partition offsets for this app + topic, then reset the state.
   */
  private def loadState(): IO[State] = {
    State.load(xa, opts.appName, topic).flatMap {
      case Some(s) => IO.pure(s)
      case None    => IO.raiseError(new Exception("failed to load state"))
    }
  }

  /**
   * Convert all the records into the desired type for the process function.
   */
  private def deserializeRecords(records: Consumer.Records): Seq[A] = {
    records.iterator.asScala.map(fromRecord).toSeq
  }

  /**
   * Constantly grab the last set of records processed and try to update -
   * and save to the database - a new state. If no new records are there,
   * just wait a little while before checking again.
   */
  private def updateState(state: State, ref: Ref[IO, Option[Consumer.Records]]): IO[Unit] = {
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
   * Before the consumer can start consuming records it must seek to the
   * correct offset for each partition, which initializes the state. If
   * the reset flag was passed on the command line then `reset` is true
   * and forced, otherwise it attempts to load the last state from MySQL.
   */
  def assignPartitions(): IO[State] = {
    val getState = if (opts.reset()) resetState() else loadState()

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
   * Create a Stream that will continuously read from Kafka and pass the
   * records to a process function.
   */
  def consume[R](state: State, process: Seq[A] => IO[R]): IO[Unit] = {
    val fetch = IO {
      client.poll(Long.MaxValue)
    }

    for {
      ref <- Ref[IO].of[Option[Consumer.Records]](None)

      // create tasks to save the state and another to process the stream
      saveTask = updateState(state, ref)
      streamTask = Stream
        .eval(fetch)
        .repeat
        .evalMap(rs => process(deserializeRecords(rs)) >> ref.set(Some(rs)))
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

/**
 * Companion object.
 */
object Consumer {

  /**
   * Helper types since the template parameters are constant.
   */
  type Record  = ConsumerRecord[String, String]
  type Records = ConsumerRecords[String, String]
}
