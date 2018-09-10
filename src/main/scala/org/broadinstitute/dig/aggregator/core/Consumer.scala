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

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

/**
 * Kafka JSON topic record consumer.
 */
final class Consumer(opts: Opts, topic: String) extends LazyLogging {

  /**
   * Create a connection to the database for writing state.
   */
  private val xa = opts.config.db.newTransactor

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
  private val client: KafkaConsumer[String, String] = {
    Thread.currentThread.setContextClassLoader(null)
    new KafkaConsumer(props)
  }

  /**
   * Get all the partitions for this topic.
   */
  private val partitions: Seq[Int] = client.partitionsFor(topic).asScala.map(_.partition)

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
    val topicPartitions  = partitions.map(new TopicPartition(topic, _))
    val beginningOffsets = client.beginningOffsets(topicPartitions.asJava)

    // create a map of offsets from Kafka
    val offsets = beginningOffsets.asScala.map {
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
   * in Kafka. So, we first find all the
   */
  private def assignPartitions(state: State): IO[State] = IO {
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

      // assign all the partitions before consuming
      latestState <- assignPartitions(state)

      // create tasks to save the state and another to process the stream
      saveTask = updateState(latestState, ref)
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
