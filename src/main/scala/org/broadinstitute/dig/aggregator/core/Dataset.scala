package org.broadinstitute.dig.aggregator.core

import cats._
import cats.effect._
import cats.implicits._

import doobie._
import doobie.implicits._

import org.json4s._
import org.json4s.JsonDSL.WithBigDecimal._
import org.json4s.jackson.JsonMethods._

/**
 * Dataset rows in the database indicate which datasets (from which topic)
 * have been processed by which apps.
 *
 * This is the implicit list of what has been - and what needs to be - done
 * at any given point in time.
 *
 * For various processors, they run after another processor. So, to get a list
 * of what needs to be processed, simply fetch the list of what's been done by
 * the dependency processor, and filter out what hasn't yet been processed by
 * this processor.
 */
case class Dataset(app: Option[String], topic: String, dataset: String, commit: Long) {

  /**
   * Insert this dataset to the database.
   */
  def insert(xa: Transactor[IO]): IO[Int] = {
    assert(app.isDefined, "Cannot insert Dataset with no application")

    val q = sql"""|INSERT INTO `datasets`
                  |  ( `app`
                  |  , `topic`
                  |  , `dataset`
                  |  , `commit`
                  |  )
                  |
                  |VALUES
                  |  ( $app
                  |  , $topic
                  |  , $dataset
                  |  , $commit
                  |  )
                  |ON DUPLICATE KEY UPDATE
                  |  `commit` = VALUES(`commit`)
                  |""".stripMargin.update

    /*
     * NB: This should raise an exception if `app` is None! The field is
     *     required in the DB schema, but is optional in the code, because
     *     Dataset.fromCommit has no source application that processed it.
     *     This is intentional and OK.
     */

    q.run.transact(xa)
  }
}

/**
 * Companion object for determining what datasets have been processed and
 * have yet to be processed.
 */
object Dataset {
  private implicit val formats: Formats = DefaultFormats

  /**
   * Parse a record from the `commits` topic into a Dataset that can be
   * inserted into the database.
   */
  def fromRecord(app: String)(record: Consumer.Record): Dataset = {
    require(record.topic == "commits", "Cannot create Dataset from topic `${record.topic}`")

    val json = parse(record.value)

    Dataset(
      app = Some(app),
      topic = (json \ "topic").extract[String],
      dataset = (json \ "dataset").extract[String],
      commit = record.offset
    )
  }

  /**
   * Convert a `Commit` to a `Dataset`.
   */
  def fromCommit(commit: Commit): Dataset = {
    Dataset(
      app = None,
      topic = commit.topic,
      dataset = commit.dataset,
      commit = commit.commit
    )
  }

  /**
   * Get all the datasets processed by a given app. This is how to find the set
   * of work for a processor to do when --reprocess-all is passed on the commend
   * line.
   */
  def datasets(xa: Transactor[IO], topic: String, app: String): IO[Seq[Dataset]] = {
    val q = sql"""|SELECT `app`,
                  |       `topic`,
                  |       `dataset`,
                  |       `commit`,
                  |
                  |FROM   `datasets`
                  |
                  |WHERE  `app` = $app
                  |AND    `topic` = $topic
                  |""".stripMargin.query[Dataset].to[Seq]

    q.transact(xa)
  }

  /**
   * Find all the datasets processed by application dependency (`dep`), but not
   * yet processed by `app`. This is how to find the set of work that a
   * processor has yet to do.
   */
  def datasets(xa: Transactor[IO], topic: String, dep: String, app: String): IO[Seq[Dataset]] = {
    val q = sql"""|SELECT          d.`app`,
                  |                d.`topic`,
                  |                d.`dataset`,
                  |                d.`commit`
                  |
                  |FROM            `datasets` d
                  |
                  |LEFT OUTER JOIN `datasets` r
                  |ON              r.`app` = $app
                  |AND             r.`topic` = $topic
                  |AND             r.`dataset` = d.`dataset`
                  |AND             r.`commit` >= d.`commit`
                  |
                  |WHERE           d.`app` = $dep
                  |AND             d.`topic` = $topic
                  |AND             r.`app` IS NULL
                  |""".stripMargin.query[Dataset].to[Seq]

    q.transact(xa)
  }
}
