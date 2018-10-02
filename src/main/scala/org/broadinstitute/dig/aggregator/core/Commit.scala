package org.broadinstitute.dig.aggregator.core

import cats._
import cats.effect._
import cats.implicits._

import doobie._
import doobie.implicits._

import java.io.File
import java.io.PrintWriter

import org.apache.kafka.clients.consumer.ConsumerRecord

import org.json4s._
import org.json4s.JsonDSL.WithBigDecimal._
import org.json4s.jackson.JsonMethods._

import scala.collection.JavaConverters._
import scala.io.Source

/**
 * Data processors (e.g. the variant processor) - when a dataset is complete -
 * post a message to the `commits` topic. This message is written by the commit
 * processor to a MySQL database so that a record is kept of every dataset
 * that exists in HDFS and can be processed.
 *
 * Dataset aggregators also listen to the `commits` topic and process entire
 * datasets upon receiving the message.
 *
 * As aggregators are created, it is their resposibility - upon first run - to
 * query all the existing datasets from the database (`Commit.datasets`) for
 * the topic they care about and process them before starting to listen to the
 * `commits` topic for new datasets.
 */
case class Commit(
    commit: Long, // commits topic offset
    topic: String, // dataset topic
    partition: Int, // dataset topic partition
    offset: Long, // dataset topic offset
    dataset: String // dataset name
) {

  /**
   * Insert this commit to the database, optionally updating if the
   * dataset+version already exists for this topic.
   */
  def insert(xa: Transactor[IO]): IO[Int] = {
    val q = sql"""INSERT INTO `commits`
                 |  ( `commit`
                 |  , `topic`
                 |  , `partition`
                 |  , `offset`
                 |  , `dataset`
                 |  )
                 |
                 |VALUES
                 |  ( $commit
                 |  , $topic
                 |  , $partition
                 |  , $offset
                 |  , $dataset
                 |  )
                 |
                 |ON DUPLICATE KEY UPDATE
                 |  `commit`    = VALUES(`commit`),
                 |  `partition` = VALUES(`partition`),
                 |  `offset`    = VALUES(`offset`),
                 |  `timestamp` = NOW()
                 |""".stripMargin.update

    q.run.transact(xa)
  }
}

/**
 * Companion object for creating, loading, and saving Commits.
 */
object Commit {
  private implicit val formats = DefaultFormats

  // TODO: commit.post with a lazy producer would be nice!

  /**
   * Create a new Commit message that can be sent to the `commits` topic.
   */
  def message(record: Consumer.Record, dataset: String): String = {
    require(!record.topic.equals("commits"))

    // cannot use Commit object here since `commit` offset doesn't exist yet
    val json =
      ("topic"       -> record.topic) ~
        ("partition" -> record.partition) ~
        ("offset"    -> record.offset) ~
        ("dataset"   -> dataset)

    // convert the JSON object to a string for the commits topic
    compact(render(json))
  }

  /**
   * Parse a record from the `commits` topic into a Commit that can be
   * inserted into the database.
   */
  def fromRecord(record: Consumer.Record): Commit = {
    require(record.topic == "commits", "Cannot create Commit from topic `${record.topic}`")

    val json = parse(record.value)

    Commit(
      commit = record.offset,
      topic = (json \ "topic").extract[String],
      partition = (json \ "partition").extract[Int],
      offset = (json \ "offset").extract[Long],
      dataset = (json \ "dataset").extract[String]
    )
  }

  /**
   * Get all the datasets committed for a given topic.
   */
  def commits(xa: Transactor[IO], topic: String): IO[Seq[Commit]] = {
    val q = sql"""|SELECT    `commit`,
                  |          `topic`,
                  |          `partition`,
                  |          `offset`,
                  |          `dataset`
                  |
                  |FROM      `commits`
                  |
                  |WHERE     `topic` = $topic
                  |
                  |ORDER BY  `commit`
                  |""".stripMargin.query[Commit].to[Seq]

    q.transact(xa)
  }

  /**
   * Get all datasets committed for a given topic not yet processed by an app.
   */
  def commits(xa: Transactor[IO], topic: String, notProcessedBy: String): IO[Seq[Commit]] = {
    val q = sql"""|SELECT           c.`commit`,
                  |                 c.`topic`,
                  |                 c.`partition`,
                  |                 c.`offset`,
                  |                 c.`dataset`
                  |
                  |FROM             `commits` AS c
                  |
                  |LEFT OUTER JOIN  `datasets` AS d
                  |ON               d.`app` = $notProcessedBy
                  |AND              d.`topic` = c.`topic`
                  |AND              d.`dataset` = c.`dataset`
                  |AND              d.`commit` = c.`commit`
                  |
                  |WHERE            c.`topic` = $topic
                  |AND              d.`app` IS NULL
                  |
                  |ORDER BY         c.`commit`
                  |""".stripMargin.query[Commit].to[Seq]

    q.transact(xa)
  }
}
