package org.broadinstitute.dig.aggregator.core

import cats.effect._

import doobie.implicits._

import org.broadinstitute.dig.aggregator.core.processors.Processor

import org.json4s._

/** Data processors (e.g. the variant processor) - when a dataset is complete -
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
case class Dataset(dataset: String, topic: String) extends Run.Input {

  /** Used for showing work in the log.
    */
  override def toString: String = s"Topic '$topic' dataset '$dataset'"

  /** The name of the dataset is the input to a run result.
    */
  override def asRunInput: String = dataset

  /** Insert this dataset to the database. This should only be done after all
    * its part files have been completely uploaded to HDFS.
    */
  def insert(pool: DbPool): IO[Int] = {
    val q = sql"""INSERT INTO `commits`
                 |  ( `dataset`
                 |  , `topic`
                 |  )
                 |
                 |VALUES
                 |  ( $dataset
                 |  , $topic
                 |  )
                 |
                 |ON DUPLICATE KEY UPDATE
                 |  `timestamp` = NOW()
                 |""".stripMargin.update

    pool.exec(q.run)
  }
}

/** Companion object for creating, loading, and saving Datasets.
  */
object Dataset {
  private implicit val formats: DefaultFormats = DefaultFormats

  /** Get all the datasets committed for a given topic.
    */
  private def datasetsOf(pool: DbPool, topic: String): IO[Seq[Dataset]] = {
    val q = sql"""|SELECT    `dataset`, `topic`
                  |FROM      `datasets`
                  |
                  |WHERE     `topic` = $topic
                  |
                  |ORDER BY  `timestamp`
                  |""".stripMargin.query[Dataset].to[Seq]

    pool.exec(q)
  }

  /** Get all datasets committed for a given topic not yet processed by an app.
    */
  private def datasetsOf(pool: DbPool, topic: String, notProcessedBy: Processor.Name): IO[Seq[Dataset]] = {
    val q = sql"""|SELECT           `datasets`.`dataset`,
                  |                 `datasets`.`topic`
                  |
                  |FROM             `datasets`
                  |
                  |LEFT OUTER JOIN  `runs`
                  |ON               `runs`.`app` = $notProcessedBy
                  |AND              `runs`.`input` = `datasets`.`dataset`
                  |AND              `runs`.`timestamp` > `datasets`.`timestamp`
                  |
                  |WHERE            `datasets`.`topic` = $topic
                  |AND              `runs`.`app` IS NULL
                  |
                  |ORDER BY         `datasets`.`timestamp`
                  |""".stripMargin.query[Dataset].to[Seq]

    pool.exec(q)
  }

  /** Helper function where the "notProcessedBy" is optional and calls the
    * correct query accordingly.
    */
  def datasetsOf(pool: DbPool, topic: String, notProcessedBy: Option[Processor.Name]): IO[Seq[Dataset]] = {
    notProcessedBy match {
      case Some(app) => datasetsOf(pool, topic, app)
      case None      => datasetsOf(pool, topic)
    }
  }
}
