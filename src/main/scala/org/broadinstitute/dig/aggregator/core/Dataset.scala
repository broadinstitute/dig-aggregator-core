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
    step: String,
    commit: Long) extends Insertable {
  
  /**
   * Insert this dataset to the database.
   */
  override def insert(xa: Transactor[IO]): IO[Int] = {
    import doobie.implicits._
    
    val q = sql"""|INSERT INTO `datasets`
                  |  ( `app`
                  |  , `topic`
                  |  , `dataset`
                  |  , `step`
                  |  , `commit`
                  |  )
                  |
                  |VALUES
                  |  ( $app
                  |  , $topic
                  |  , $dataset
                  |  , $step
                  |  , $commit
                  |  )
                  |""".stripMargin.update

    q.run.transact(xa)
  }
}
