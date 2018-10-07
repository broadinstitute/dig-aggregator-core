package org.broadinstitute.dig.aggregator.core

import cats._
import cats.effect._
import cats.implicits._

import doobie._
import doobie.implicits._

import org.broadinstitute.dig.aggregator.core.processors.Processor

/**
 * Every time a processor "runs" over a set of inputs, it produces an output.
 * Those inputs -> output are recorded in the `runs` table. If a processor
 * runs multiple times in a quick series to produce multiple outputs, then
 * each of the rows produced will have the same run `id`, indicating that
 * they came from processing the same batch of data.
 */
case class Run(run: Long, app: Processor.Name, input: String, output: String)

/**
 * Companion object for determining what inputs have been processed and
 * have yet to be processed.
 */
object Run {
  import Processor.NameMeta

  /**
   * Run entries are created and inserted atomically for a single output.
   */
  def insert(xa: Transactor[IO], app: Processor.Name, inputs: Seq[String], output: String): IO[Int] = {
    val sql = """|INSERT INTO `runs`
                 |  ( `run`
                 |  , `app`
                 |  , `input`
                 |  , `output`
                 |  )
                 |
                 |VALUES
                 |  (?, ?, ?, ?)
                 |
                 |ON DUPLICATE KEY UPDATE
                 |  `run` = VALUES(`run`),
                 |  `output` = VALUES(`output`),
                 |  `timestamp` = NOW()
                 |""".stripMargin

    // create a run for each input, use the time in milliseconds as the ID
    val runId = System.currentTimeMillis()
    val runs  = inputs.map(Run(runId, app, _, output))

    // run the update atomically, insert a row per input for this output
    Update[Run](sql).updateMany(runs.toList).transact(xa)
  }

  /**
   * When querying the `runs` table to determine what has already been
   * processed by dependency applications, this is what is returned.
   */
  final case class Result(app: Processor.Name, output: String, timestamp: java.time.Instant)

  /**
   * Given a list of applications, determine all the outputs produced by all
   * of them together.
   */
  def results(xa: Transactor[IO], deps: Seq[Processor.Name]): IO[Seq[Result]] = {
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
  def results(xa: Transactor[IO], deps: Seq[Processor.Name], notProcessedBy: Processor.Name): IO[Seq[Result]] = {
    val selects = deps.map { dep =>
      fr"SELECT `app`, `output`, `timestamp` FROM runs WHERE app=$dep"
    }

    // join all the dependencies together
    val union  = selects.toList.intercalate(fr"UNION ALL")
    val select = fr"SELECT `inputs`.`app`, `inputs`.`output`, MAX(`inputs`.`timestamp`) AS `timestamp`"
    val from   = fr"FROM (" ++ union ++ fr") AS `inputs`"
    val where  = fr"WHERE r.`app` IS NULL"
    val group  = fr"GROUP BY `inputs`.`app`, `inputs`.`output`"
    val join   = fr"""|LEFT OUTER JOIN runs AS r
                      |ON r.`app` = $notProcessedBy
                      |AND r.`input` = inputs.`output`
                      |AND r.`timestamp` > inputs.`timestamp`
                      |""".stripMargin

    // join all the fragments together the query
    val q = (select ++ from ++ join ++ where ++ group).query[Result].to[Seq]

    // run the query
    q.transact(xa)
  }

  /**
   * Helper function where the "notProcessedBy" is optional and calls the
   * correct query accordingly.
   */
  def results(xa: Transactor[IO], deps: Seq[Processor.Name], notProcessedBy: Option[Processor.Name]): IO[Seq[Result]] = {
    notProcessedBy match {
      case Some(app) => results(xa, deps, app)
      case None      => results(xa, deps)
    }
  }
}
