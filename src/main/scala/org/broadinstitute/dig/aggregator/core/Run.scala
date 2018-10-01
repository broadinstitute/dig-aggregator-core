package org.broadinstitute.dig.aggregator.core

import java.time.Instant
import doobie.util.transactor.Transactor
import cats.effect.IO

/**
 * @author clint
 * Oct 1, 2018
 */
case class Run(app: String, timestamp: Instant, data: String) {
  /**
   * Insert this run to the database.
   */
  def insert(xa: Transactor[IO]): IO[Int] = {
    import doobie.implicits._
    
    val q = sql"""|INSERT INTO `runs`
                  |  ( `app`
                  |  , `timestamp`
                  |  , `data`
                  |  )
                  |
                  |VALUES
                  |  ( $app
                  |  , $timestamp
                  |  , $data
                  |  )
                  |ON DUPLICATE KEY UPDATE
                  |   `timestamp` = VALUES(`timestamp`)
                  |  ,`data`    = VALUES(`data`)
                  |""".stripMargin.update

    q.run.transact(xa)
  }
}

object Run {
  def lastRunForApp(xa: Transactor[IO])(app: String): IO[Option[Run]] = {
    import doobie.implicits._
    
    val q = sql"""|SELECT
                  |    `app`
                  |  , `timestamp`
                  |  , `data`
                  |FROM `runs`
                  |WHERE `app` = $app
                  |LIMIT 1
                  |""".stripMargin.query[Run].to[Seq].map(_.headOption)

    q.transact(xa)
  }
}
