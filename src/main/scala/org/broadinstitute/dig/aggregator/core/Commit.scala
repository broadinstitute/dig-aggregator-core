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
    dataset: String, // dataset name
    version: Int // dataset version
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
                 |  , `version`
                 |  )
                 |
                 |VALUES
                 |  ( $commit
                 |  , $topic
                 |  , $partition
                 |  , $offset
                 |  , $dataset
                 |  , $version
                 |  )
                 |
                 |ON DUPLICATE KEY UPDATE
                 |  `commit`    = VALUES(`commit`)
                 |  `partition` = VALUES(`partition`),
                 |  `offset`    = VALUES(`offset`),
                 |  `record`    = VALUES(`record`),
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

  // pattern for splitting a dataset/version into its parts
  private val idVer = raw"([^/]+)/(\d+)".r

  // TODO: commit.post with a lazy producer would be nice!

  /**
   * Create a new Commit message that can be sent to the `commits` topic.
   */
  def message(record: Consumer#Record, datasetVersion: String): String = {
    require(!record.topic.equals("commits"))

    // extract the dataset ID and version
    val idVer(dataset, version) = datasetVersion

    // cannot use Commit object here since `commit` offset doesn't exist yet
    val json =
      ("topic"       -> record.topic) ~
        ("partition" -> record.partition) ~
        ("offset"    -> record.offset) ~
        ("dataset"   -> dataset) ~
        ("version"   -> version.toInt)

    // convert the JSON object to a string for the commits topic
    compact(render(json))
  }

  /**
   * Parse a record from the `commits` topic into a Commit that can be
   * inserted into the database.
   */
  def fromCommitRecord(record: Consumer#Record): Commit = {
    require(record.topic.equals("commits"))

    val json = parse(record.value)

    println(record.value)
    println(json)

    println((json \ "topic").extract[String])
    println((json \ "partition").extract[Int])
    println((json \ "offset").extract[Long])
    println((json \ "dataset").extract[String])
    println((json \ "version").extract[Int])

    Commit(
      commit = record.offset,
      topic = (json \ "topic").extract[String],
      partition = (json \ "partition").extract[Int],
      offset = (json \ "offset").extract[Long],
      dataset = (json \ "dataset").extract[String],
      version = (json \ "version").extract[Int]
    )
  }

  /**
   * In the commits table are all the datasets that have been committed for
   * a given source topic. When a new dataset processor app is created (or
   * needs to start over), this function can get all the datasets that have
   * already been committed as well as the last commit offset so the processor
   * knows where to start consuming from.
   */
  def datasets(xa: Transactor[IO], topic: String): IO[List[Commit]] = {
    val q = sql"""SELECT   `commit`, `topic`, `partition`, `offset`, `dataset`, `version`
                 |FROM     `commits`
                 |WHERE    `topic` = $topic
                 |ORDER BY `commit`
                 |""".stripMargin.query[Commit].to[List]

    // run the query
    q.transact(xa)
  }
}
