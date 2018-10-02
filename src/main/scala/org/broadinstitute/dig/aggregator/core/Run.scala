package org.broadinstitute.dig.aggregator.core

import cats._
import cats.effect._
import cats.implicits._

import doobie._
import doobie.implicits._

/**
 * Every time a processor "runs" over a set of inputs, it produces an output.
 * Those inputs -> output are recorded in the `runs` table. If a processor
 * runs multiple times in a quick series to produce multiple outputs, then
 * each of the rows produced will have the same run `id`, indicating that
 * they came from processing the same batch of data.
 */
case class Run(run: Long, app: String, input: String, output: String) {

  /**
   * Insert this run entry into the database.
   */
  def insert(xa: Transactor[IO]): IO[Int] = {
    val q = sql"""|INSERT INTO `runs`
                  |  ( `run`
                  |  , `app`
                  |  , `input`
                  |  , `output`
                  |  )
                  |
                  |VALUES
                  |  ( $run
                  |  , $app
                  |  , $input
                  |  , $output
                  |  )
                  |
                  |ON DUPLICATE KEY UPDATE
                  |  `run` = VALUES(`run`),
                  |  `output` = VALUES(`output`),
                  |  `timestamp` = NOW()
                  |""".stripMargin.update

    q.run.transact(xa)
  }
}

/**
 * Companion object for determining what inputs have been processed and
 * have yet to be processed.
 */
object Run {

  /**
   * When querying the `runs` table to determine what has already been
   * processed by dependency applications, this is what is returned.
   */
  final case class Result(app: String, output: String, timestamp: java.time.Instant)

  /**
   * Given a list of applications, determine all the outputs produced by all
   * of them together.
   */
  def runs(xa: Transactor[IO], deps: Seq[String]): IO[Seq[Result]] = {
    val qs = deps.map { dep =>
      sql"SELECT `app`, `output`, `timestamp` FROM runs WHERE app=$dep"
    }

    // run a select query for each application
    val results = qs.map(_.query[Result].to[Seq].transact(xa))

    // join all the results together into a single list
    results.toList.sequence.map(_.reduce(_ ++ _))
  }

  /**
   * Given an application and a list of dependency applications, determine
   * what outputs have been produced by the dependencies that have yet to be
   * processed by it.
   */
  def runs(xa: Transactor[IO], app: String, deps: Seq[String]): IO[Seq[Result]] = {
    val selects = deps.map { dep =>
      fr"SELECT `app`, `output`, `timestamp` FROM runs WHERE app=$dep"
    }

    // join all the dependencies together
    val union  = selects.toList.intercalate(fr"UNION ALL")
    val select = fr"SELECT inputs.*"
    val from   = fr"FROM (" ++ union ++ fr") AS inputs"
    val where  = fr"WHERE r.`app` IS NULL"
    val join   = fr"""|LEFT OUTER JOIN runs AS r
                      |ON r.`app` = $app
                      |AND r.`input` = inputs.`output`
                      |AND r.`timestamp` > inputs.`timestamp`
                      |""".stripMargin

    // join all the fragments together the query
    val q = (select ++ from ++ join ++ where).query[Result].to[Seq]

    // run the query
    q.transact(xa)
  }
}
