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
case class State(app: String, topic: String, offsets: Map[Int, Long]) {

  /**
   * Return a new state with a updated partition offsets.
   */
  def +(record: Consumer#Record): State = {
    require(record.topic == topic, "ConsumerRecord topic doesn't match $topic!")

    // update the offsets map
    copy(offsets = offsets + (record.partition -> record.offset))
  }

  /**
   * Return a new state with a updated partition offsets.
   */
  def ++(records: Consumer#Records): State = {
    records.iterator.asScala.foldLeft(this)(_ + _)
  }

  /**
   * Save this Kafka consumer state to the database.
   */
  def save(xa: Transactor[IO]): IO[State] = {
    val q = """INSERT INTO `offsets`
              |  ( `app`
              |  , `topic`
              |  , `partition`
              |  , `offset`
              |  )
              |
              |VALUES (?, ?, ?, ?)
              |
              |ON DUPLICATE KEY UPDATE `offset` = VALUES(`offset`)
              |""".stripMargin

    // convert the state into a set of values to insert
    val values = offsets.map {
      case (partition, offset) => (app, topic, partition, offset)
    }

    // run the query, replace existing offsets, true if db was updated
    Update[(String, String, Int, Long)](q)
      .updateMany(values.toList)
      .transact(xa)
      .map(_ => this)
  }
}

/**
 * Companion object for creating, loading, and saving ConsumerState instances.
 */
object State {
  type OffsetMap = Map[Int, Long]

  /**
   * Load a ConsumerState from MySQL. This collects all the partition/offset
   * rows for a given app+topic and merges them together. If no partition
   * offsets exist in the database for this app+topic, then `None` is returned
   * and it is expected that `State.latest` will be used to reset the state
   * of the consumer.
   */
  def load(xa: Transactor[IO], app: String, topic: String): IO[Option[State]] = {
    val q = sql"""SELECT   `partition`, `offset`+1 AS `offset`
                 |FROM     `offsets`
                 |WHERE    `app` = $app AND `topic` = $topic
                 |ORDER BY `partition`
                 |""".stripMargin.query[(Int, Long)].to[List]

    // TODO: should this function take beginning as well and verify # partitions?

    // fetch all the offsets for every partition on this topic for this app
    q.transact(xa).map { offsets =>
      if (offsets.isEmpty) None else Some(new State(app, topic, offsets.toMap))
    }
  }

  /**
   * Since the commits table knows the partition and offset of a source topic
   * that had a dataset committed, those offsets can safely be skipped by
   * data type processors. If a type processor needs to reset its state, it
   * can get the map of partitions and offsets to seek to here.
   */
  def reset(xa: Transactor[IO], app: String, topic: String, from: OffsetMap): IO[OffsetMap] = {
    val delete = sql"""DELETE FROM `offsets`
                      |WHERE       `app` = $app
                      |AND         `topic` = $topic
                      |""".stripMargin.update

    val select = sql"""SELECT   `partition`, MAX(`offset`)+1 AS `offset`
                      |FROM     `commits`
                      |WHERE    `topic` = $topic
                      |GROUP BY `partition`
                      |""".stripMargin.query[(Int, Long)].to[List]

    // delete then select in the same transaction
    val offsets = for {
      _ <- delete.run
      r <- select
    } yield from ++ r

    // execute the queries
    offsets.transact(xa)
  }
}
