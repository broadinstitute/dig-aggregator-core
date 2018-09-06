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
   * Before the consumer can start consuming records it must seek to the
   * correct offset for each partition, which initializes the state. If
   * the reset flag was passed on the command line then `reset` is true
   * and forced, otherwise it attempts to load the last state from MySQL.
   */
  private def assignPartitions(state: State): IO[Unit] = IO {
    client.assign(state.offsets.keys.map(new TopicPartition(topic, _)).toList.asJava)

    // advance the partitions to the correct offsets
    for ((partition, offset) <- state.offsets) {
      client.seek(new TopicPartition(topic, partition), offset)
    }

    // log the partition information being consumed
    logger.info(s"Consuming for topic '${state.topic}' from...")

    // sort by partition and output all the current offsets
    for ((partition, offset) <- state.offsets.toList.sortBy(_._1)) {
      logger.info(s" ..partition $partition at offset $offset")
    }
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

      // assign all the partitions before consuming
      _ <- assignPartitions(state)

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
