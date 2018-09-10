package org.broadinstitute.dig.aggregator.core

import doobie.util.transactor.Transactor
import cats.effect.IO

/**
 * @author clint
 * Aug 28, 2018
 */
case class Dataset(
    app: String,
    topic: String,
    dataset: String,
    commit: Long) {
  
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
