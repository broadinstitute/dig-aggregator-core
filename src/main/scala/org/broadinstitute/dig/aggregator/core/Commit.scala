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
      ("topic"     -> record.topic) ~
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
    require(record.topic.equals("commits"))

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
   * In the commits table are all the datasets that have been committed for
   * a given source topic. When a new dataset processor app is created (or
   * needs to start over), this function can get all the datasets that have
   * already been committed as well as the last commit offset so the processor
   * knows where to start consuming from.
   */
  def datasetCommits(xa: Transactor[IO], topic: String, ignoreProcessedBy: Option[String]): IO[Seq[Commit]] = {
    val select = fr"""|SELECT   `commits`.`commit`,
                      |`commits`.`topic`,
                      |`commits`.`partition`,
                      |`commits`.`offset`,
                      |`commits`.`dataset`
                      |FROM     `commits`""".stripMargin
      
    def makeJoin(app: String) = fr"""|LEFT JOIN   `datasets`
                                     |ON          `datasets`.`app` = $app
                                     |AND         `datasets`.`topic` = `commits`.`topic`
                                     |AND         `datasets`.`dataset` = `commits`.`dataset`
                                     |AND         `datasets`.`commit` = `commits`.`commit`""".stripMargin
    
    val whereCommitsTopic = fr"`commits`.`topic` = $topic"
    def whereDatasetsApp = fr"`datasets`.`app` IS NULL"
    
    val orderBy = fr"ORDER BY `commits`.`commit`"
    
    val (join, wheres) = ignoreProcessedBy match {
      case None =>      Fragment.empty -> Seq(whereCommitsTopic)
      case Some(app) => makeJoin(app) -> Seq(whereCommitsTopic, whereDatasetsApp)
    }
    
    val q = select ++ join ++ Fragments.whereAnd(wheres: _*) ++ orderBy
    
    q.query[Commit].to[Seq].transact(xa)
  }
}
