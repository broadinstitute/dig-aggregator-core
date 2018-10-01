package org.broadinstitute.dig.aggregator.core

import cats._
import cats.effect._
import cats.implicits._

import doobie._
import doobie.implicits._

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
case class Dataset(app: String, topic: String, dataset: String, commit: Long) {

  /**
   * Insert this dataset to the database.
   */
  def insert(xa: Transactor[IO]): IO[Int] = {
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

/**
 * Companion object for determining what datasets have been processed and
 * have yet to be processed.
 */
object Dataset {

  /**
   * Get all the datasets processed by a given app. This is how to find the set
   * of work for a processor to do when --reprocess-all is passed on the commend
   * line.
   */
  def processedBy(xa: Transactor[IO], app: String): IO[Seq[Dataset]] = {
    val q = sql"""|SELECT `app`,
                  |       `topic`,
                  |       `dataset`,
                  |       `commit`,
                  |
                  |FROM   `datasets`
                  |
                  |WHERE  `app` = $app
                  |""".stripMargin.query[Dataset].to[Seq]

    q.transact(xa)
  }

  /**
   * Find all the datasets processed by application dependency, but not
   * yet processed by another app. This is how to find the set of work that
   * a processor has yet to do.
   */
  def processedBy(xa: Transactor[IO], app: String, notProcessedBy: String): IO[Seq[Dataset]] = {
    val q = sql"""|SELECT          d.`app`,
                  |                d.`topic`,
                  |                d.`dataset`,
                  |                d.`commit`
                  |
                  |FROM            `datasets` d
                  |
                  |LEFT OUTER JOIN `datasets` r
                  |ON              r.`app` = $notProcessedBy
                  |AND             r.`dataset` = d.`dataset`
                  |AND             r.`commit` >= d.`commit`
                  |
                  |WHERE           d.`app` = $app
                  |AND             r.`app` IS NULL
                  |""".stripMargin.query[Dataset].to[Seq]

    q.transact(xa)
  }
}
