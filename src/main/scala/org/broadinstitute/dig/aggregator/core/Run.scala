package org.broadinstitute.dig.aggregator.core

import cats._
import cats.effect._
import cats.implicits._

import doobie._
import doobie.implicits._

import org.broadinstitute.dig.aggregator.core.processors.Processor

/**
 * Companion object for determining what inputs have been processed and
 * have yet to be processed.
 */
object Run {

  /**
   * An Entry represents a single row in the `runs` table.
   *
   * A processor may take several inputs to produce a single output. In such
   * an instance, multiple rows are inserted using the same id (`Entry.run`),
   * which is application-specific, but - for us - a milliseconds timestamp.
   *
   * This class is private because it's only used by Doobie to insert rows and
   * is never actually used outside of insertion. Use `Run.Result` for getting
   * data out of the `runs` table.
   */
  private case class Entry(run: Long, app: Processor.Name, input: String, output: String)

  /**
   * Run entries are created and inserted atomically for a single output.
   */
  def insert(xa: Transactor[IO], app: Processor.Name, inputs: Seq[String], output: String): IO[Long] = {
    val sql = s"""|INSERT INTO `runs`
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
    val runId   = System.currentTimeMillis()
    val entries = inputs.map(Entry(runId, app, _, output))

    for {
      _ <- Update[Entry](sql).updateMany(entries.toList).transact(xa)

      // insert the provenance row, but should be part of same transaction
      _ <- Provenance.thisBuild.insert(xa, runId, app)
    } yield runId
  }

  /**
   * When querying the `runs` table to determine what has already been
   * processed by dependency applications, this is what is returned.
   */
  final case class Result(app: Processor.Name, output: String, timestamp: java.time.Instant) {
    override def toString: String = s"$app output '$output'"
  }

  /**
   * Lookup all the results for a given run id. This is mostly used for
   * testing.
   */
  def resultsOfRun(xa: Transactor[IO], run: Long): IO[Seq[Result]] = {
    val q = sql"""|SELECT `app`, `output`, `timestamp`
                  |FROM   `runs`
                  |WHERE  `run`=$run
                  |""".stripMargin.query[Result].to[Seq]

    q.transact(xa)
  }

  /**
   * Given a list of applications, determine all the outputs produced by all
   * of them together.
   */
  private def resultsOf(xa: Transactor[IO], deps: Seq[Processor.Name]): IO[Seq[Result]] = {
    val qs = deps.map { dep =>
      sql"SELECT `app`, `output`, `timestamp` FROM `runs` WHERE app=$dep"
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
  private def resultsOf(xa: Transactor[IO],
                        deps: Seq[Processor.Name],
                        notProcessedBy: Processor.Name): IO[Seq[Result]] = {
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
  def resultsOf(xa: Transactor[IO],
                deps: Seq[Processor.Name],
                notProcessedBy: Option[Processor.Name]): IO[Seq[Result]] = {
    notProcessedBy match {
      case Some(app) => resultsOf(xa, deps, app)
      case None      => resultsOf(xa, deps)
    }
  }
}
