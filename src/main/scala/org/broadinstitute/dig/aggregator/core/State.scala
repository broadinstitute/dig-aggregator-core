package org.broadinstitute.dig.aggregator.core

import cats._
import cats.effect._
import cats.implicits._

import doobie._
import doobie.implicits._

import java.io.File
import java.io.PrintWriter

import scala.collection.JavaConverters._
import scala.io.Source

import org.apache.kafka.clients.consumer.ConsumerRecord

/**
 * The consumer state object tracks of what offset - for each partition in a
 * Kafka topic - an application has successfully processed through.
 */
class State(val app: String, val topic: String, var offsets: Map[Int, Long]) {

  /**
   * Update the current state from a record that's been processed.
   */
  def update(record: ConsumerRecord[_, _]) = {
    require(record.topic == topic, "ConsumerRecord topic doesn't match $topic!")

    // update the offsets map
    offsets += record.partition -> record.offset
  }

  /**
   * Save this Kafka consumer state to the database.
   */
  def save(xa: Transactor[IO]): IO[Boolean] = {
    val q = """INSERT INTO partitions (app, topic, partition, offset)
              |VALUES (?, ?, ?, ?)
              |ON DUPLICATE KEY UPDATE offset = VALUES(offset)
              |""".stripMargin

    // convert the state into a set of values to insert
    val values = offsets.map {
      case (partition, offset) => (app, topic, partition, offset)
    }

    // run the query, replace existing offsets, true if db was updated
    Update[(String, String, Int, Long)](q)
      .updateMany(values.toList)
      .transact(xa)
      .map(_ > 0)
  }
}

/**
 * Companion object for creating, loading, and saving ConsumerState instances.
 */
object State {

  /**
   * Load a ConsumerState from MySQL. This collects all the partition/offset
   * rows for a given app+topic and merges them together. If no partition
   * offsets exist in the database for this app+topic, then `None` is returned
   * and it is expected that `State.latest` will be used to reset the state
   * of the consumer.
   */
  def load(xa: Transactor[IO], app: String, topic: String): IO[Option[State]] = {
    val q = sql"""SELECT partition, offset FROM partitions
                 |WHERE app = $app AND topic = $topic
                 |ORDER BY partition
                 |""".stripMargin.query[(Int, Long)].to[List]

    // TODO: should this function take beginning as well and verify # partitions?

    // fetch all the offsets for every partition on this topic for this app
    q.transact(xa).map { offsets =>
      if (offsets.isEmpty) None else Some(new State(app, topic, offsets.toMap))
    }
  }

  /**
   * In the commits table are all the datasets that have been committed for
   * a given source topic. Instead of processing from the beginning or end,
   * when resetting (or creating a new consumer), it's best to query the
   * commits table for the "latest" offsets across partitions, skipping all
   * the datasets that have already been processed.
   */
  def latest(xa: Transactor[IO], topic: String, beginning: Map[Int, Long]): IO[Map[Int, Long]] = {
    val q = sql"""SELECT partition, MAX(offset) AS offset FROM commits
                        |WHERE topic = $topic
                        |GROUP BY partition
                        |""".stripMargin.query[(Int, Long)].to[List]

    // fetch a map of all the latest partition/offsets for this source
    q.transact(xa).map(_.foldLeft(beginning)(_ + _))
  }
}
