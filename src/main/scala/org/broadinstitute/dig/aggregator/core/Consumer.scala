package org.broadinstitute.dig.aggregator.core

import cats._
import cats.effect._
import cats.effect.concurrent._
import cats.implicits._

import com.typesafe.scalalogging.LazyLogging

import doobie._

import fs2.Stream

import java.io.File
import java.util.Properties

import org.apache.kafka.clients.consumer._
import org.apache.kafka.common._

import org.broadinstitute.dig.aggregator.core.config.KafkaConfig

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

/**
 * Kafka JSON topic record consumer.
 */
final class Consumer(config: KafkaConfig, val topic: String, xa: Transactor[IO]) extends LazyLogging {

  /**
   * Kafka connection properties.
   */
  private val props: Properties = utils.Props(
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG        -> config.brokerList,
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG   -> classOf[serialization.StringDeserializer].getCanonicalName,
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[serialization.StringDeserializer].getCanonicalName
  )

  /**
   * The Kafka client used to receive variant JSON messages.
   */
  private val client: KafkaConsumer[String, String] = {
    Thread.currentThread.setContextClassLoader(null)

    /*
     * This is a hack so that Kafka can find the deserializer classes!
     */

    new KafkaConsumer(props)
  }

  /**
   * Get all the partitions for this topic.
   */
  val partitions: Seq[Int] = {
    client.partitionsFor(topic).asScala.map(_.partition)
  }

  /**
   * All the partitions for this Topic as Kafka TopicPartitions.
   */
  val topicPartitions: Seq[TopicPartition] = {
    partitions.map(new TopicPartition(topic, _))
  }

  /**
   * A Map of TopicPartition -> offset (Long) given the earliest offsets
   * available in Kafka for this topic.
   */
  def beginningOffsets: Map[TopicPartition, Long] = client
    .beginningOffsets(topicPartitions.asJava)
    .asScala
    .mapValues(_.toLong)
    .toMap

  /**
   * A Map of TopicPartition -> offset (Long) given the latest offsets
   * available in Kafka for this topic.
   */
  def endOffsets: Map[TopicPartition, Long] = client
    .endOffsets(topicPartitions.asJava)
    .asScala
    .mapValues(_.toLong)
    .toMap

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
   * The state that is loaded from the database may not actually contain all
   * partitions for a given topic. For that reason, we have to ask Kafka for
   * all the partitions and their beginning offsets, then overwrite those
   * with updated ones from the State.
   */
  private def mergeStateOffsets(state: State): State = {
    val offsets = beginningOffsets.map {
      case (topicPartition, offset) => topicPartition.partition -> offset.toLong
    }

    // override the beginning offsets with the state offsets loaded
    state.copy(offsets = offsets.toMap ++ state.offsets)
  }

  /**
   * Before the consumer can start consuming records it must seek to the
   * correct offset for each partition, which initializes the state.
   *
   * The state that was loaded may not contain all the partitions because it
   * either isn't there in the database or the number of partitions changed
   * in Kafka. So, we first find all the offsets that Kafka knows about and
   * merge them with the ones loaded from the database (using the database as
   * as the authority on where consumption should resume from).
   */
  def assignPartitions(state: State): IO[State] = IO {
    val updatedState = mergeStateOffsets(state)
    val topicPartitions = updatedState.offsets.keys.map {
      new TopicPartition(topic, _)
    }

    // assign all the partitions
    client.assign(topicPartitions.toList.asJava)

    // seek to the correct offsets
    for ((partition, offset) <- updatedState.offsets) {
      client.seek(new TopicPartition(topic, partition), offset)
    }

    // log the partition information being consumed
    logger.info(s"Consuming for topic '${updatedState.topic}' from...")

    // sort by partition and output all the current offsets
    for ((partition, offset) <- updatedState.offsets.toList.sortBy(_._1)) {
      logger.info(s"...partition $partition at offset $offset")
    }

    // return the state with all the offsets
    updatedState
  }

  /**
   * Create a Stream that will continuously read from Kafka and pass the
   * records to a process function.
   */
  def consume(state: State, process: Seq[Consumer.Record] => IO[_]): IO[Unit] = {
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
        .evalMap(rs => process(rs.iterator.asScala.toSeq) >> ref.set(Some(rs)))
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
