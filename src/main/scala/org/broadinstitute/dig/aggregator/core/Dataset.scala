package org.broadinstitute.dig.aggregator.core

import cats.effect.IO

import doobie.util.transactor.Transactor

import org.json4s._
import org.json4s.JsonDSL.WithBigDecimal._
import org.json4s.jackson.JsonMethods._

/**
 * @author clint
 * Aug 28, 2018
 */
case class Dataset(app: String, topic: String, dataset: String, commit: Long) {

  /**
   * Insert this dataset to the database.
   */
  def insert(xa: Transactor[IO]): IO[Int] = {
    import doobie.implicits._

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
                  |  `commit`    = VALUES(`commit`)
                  |""".stripMargin.update

    q.run.transact(xa)
  }
}

object Dataset {
  private implicit val formats: Formats = DefaultFormats

  /**
   * Parse a record from the `commits` topic into a Dataset that can be
   * inserted into the database.
   */
  def fromRecord(app: String)(record: Consumer.Record): Dataset = {
    require(record.topic == "commits", "Cannot create Dataset from topic other than `commits`")

    val json = parse(record.value)

    Dataset(
      app = app,
      topic = (json \ "topic").extract[String],
      dataset = (json \ "dataset").extract[String],
      commit = record.offset
    )
  }

  /**
   * Convert a `Commit` to a `Dataset`.
   */
  def fromCommit(app: String)(commit: Commit): Dataset = {
    Dataset(
      app = app,
      topic = commit.topic,
      dataset = commit.dataset,
      commit = commit.commit
    )
  }
}
