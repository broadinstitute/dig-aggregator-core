package org.broadinstitute.dig.aggregator.core

import cats.effect._
import cats.implicits._
import com.typesafe.scalalogging.LazyLogging
import doobie._
import doobie.implicits._
import java.util.UUID.randomUUID

import org.broadinstitute.dig.aggregator.core.processors._

/** Companion object for determining what inputs have been processed and
  * have yet to be processed.
  */
object Run extends LazyLogging {

  /** An Entry represents a single row in the `runs` table.
    *
    * A processor may take several inputs to produce a single output. In such
    * an instance, multiple rows are inserted using the same id (`Entry.run`),
    * which is application-specific, but - for us - a milliseconds timestamp.
    *
    * This class is private because it's only used by doobie to insert rows and
    * is never actually used outside of insertion. Use `Run.Result` for getting
    * data out of the `runs` table.
    */
  private case class Entry(
      run: String,
      app: Processor.Name,
      input: String,
      output: String,
      source: String,
      branch: String,
      commit: String
  )

  /** Run entries are created and inserted atomically for a single output.
    */
  def insert(pool: DbPool, app: Processor.Name, output: String, inputs: Seq[String]): IO[String] = {
    val q = s"""|INSERT INTO `runs`
                |  ( `run`
                |  , `app`
                |  , `input`
                |  , `output`
                |  , `source`
                |  , `branch`
                |  , `commit`
                |  )
                |
                |VALUES (?, ?, ?, ?, ?, ?, ?)
                |
                |ON DUPLICATE KEY UPDATE
                |  `run` = VALUES(`run`),
                |  `source` = VALUES(`source`),
                |  `branch` = VALUES(`branch`),
                |  `commit` = VALUES(`commit`),
                |  `timestamp` = NOW()
                |""".stripMargin

    // generate the run ID and an insert-multi update
    val runId = randomUUID.toString
    val prov  = Provenance.thisBuild

    // create an entry per input
    val entries = inputs.map { input =>
      Entry(runId, app, input, output, prov.source, prov.branch, prov.commit)
    }

    val insert = Update[Entry](q).updateMany(entries.toList)

    for {
      _ <- IO(logger.debug(s"Inserting run output '$output' for $app..."))
      _ <- pool.exec(insert)
    } yield runId
  }

  /** Given a processor and an output, delete it from the DB.
    */
  def delete(pool: DbPool, app: Processor.Name, output: String): IO[Unit] = {
    val q = sql"DELETE FROM `runs` WHERE app=$app AND output=$output"

    for {
      _ <- pool.exec(q.update.run)
    } yield ()
  }

  /** Anything that can be represented as an input to a run.
    */
  trait Input {
    def asRunInput: String
  }

  /** When querying the `runs` table to determine what has already been
    * processed by dependency applications, this is what is returned.
    */
  final case class Result(app: Processor.Name, input: String, output: String, timestamp: java.time.Instant)
      extends Run.Input {
    override def toString: String = s"$app output '$output'"

    /** The output of this run result is the input to another run.
      */
    override def asRunInput: String = output
  }

  /** Lookup all the results for a given run id. This is mostly used for
    * testing.
    */
  def resultsOfRun(pool: DbPool, run: String): IO[Seq[Result]] = {
    val q = sql"""|SELECT `app`, `input`, `output`, `timestamp`
                  |FROM   `runs`
                  |WHERE  `run`=$run
                  |""".stripMargin.query[Result].to[Seq]

    pool.exec(q)
  }

  /** Given a list of applications, determine all the outputs produced by all
    * of them together.
    */
  private def resultsOf(pool: DbPool, deps: Seq[Processor.Name]): IO[Seq[Result]] = {
    val selects = deps.map { dep =>
      fr"SELECT `app`, `input`, `output`, `timestamp` FROM `runs` WHERE `app`=$dep"
    }

    // join all the dependencies together
    val union = selects.toList.intercalate(fr"UNION ALL")
    val select =
      fr"SELECT `inputs`.`app`, `inputs`.`input`, `inputs`.`output`, MAX(`inputs`.`timestamp`) AS `timestamp`"
    val from  = fr"FROM (" ++ union ++ fr") AS `inputs`"
    val group = fr"GROUP BY `inputs`.`app`, `inputs`.`output`"

    // run a select query for each application
    val q = (select ++ from ++ group).query[Result].to[Seq]

    // join all the results together into a single list
    pool.exec(q)
  }

  /** Given an application and a list of dependency applications, determine
    * what outputs have been produced by the dependencies that have yet to be
    * processed by it.
    */
  private def resultsOf(pool: DbPool, deps: Seq[Processor.Name], notProcessedBy: Processor.Name): IO[Seq[Result]] = {
    val selects = deps.map { dep =>
      fr"SELECT `app`, `input`, `output`, `timestamp` FROM `runs` WHERE `app`=$dep"
    }

    // join all the dependencies together
    val union = selects.toList.intercalate(fr"UNION ALL")
    val select =
      fr"SELECT `inputs`.`app`, `inputs`.`input`, `inputs`.`output`, MAX(`inputs`.`timestamp`) AS `timestamp`"
    val from  = fr"FROM (" ++ union ++ fr") AS `inputs`"
    val where = fr"WHERE r.`app` IS NULL"
    val group = fr"GROUP BY `inputs`.`app`, `inputs`.`output`"
    val join  = fr"""|LEFT OUTER JOIN runs AS r
                     |ON r.`app` = $notProcessedBy
                     |AND r.`input` = inputs.`output`
                     |AND r.`timestamp` > inputs.`timestamp`
                     |""".stripMargin

    // join all the fragments together the query
    val q = (select ++ from ++ join ++ where ++ group).query[Result].to[Seq]

    // run the query
    pool.exec(q)
  }

  /** Helper function where the "notProcessedBy" is optional and calls the
    * correct query accordingly.
    */
  def resultsOf(pool: DbPool, deps: Seq[Processor.Name], notProcessedBy: Option[Processor.Name]): IO[Seq[Result]] = {
    notProcessedBy match {
      case Some(app) => resultsOf(pool, deps, app)
      case None      => resultsOf(pool, deps)
    }
  }

  /** Validate the runs of a given processor and return a list of outputs that
    * are no longer valid. An run's output is considered invalid if none of the
    * inputs are valid (e.g. they were deleted). If a run's output is invalid
    * then it can be deleted.
    */
  def verifyResultsOf(pool: DbPool, processor: Processor[_]): IO[Seq[String]] = {
    val getInputs = processor match {
      case dp: DatasetProcessor => Dataset.datasetsOf(pool, dp.topic, None).map(_.map(_.dataset))
      case rp: RunProcessor     => resultsOf(pool, rp.dependencies).map(_.map(_.output))
    }

    for {
      allResults <- resultsOf(pool, Seq(processor.name)).map(_.groupBy(_.output))
      inputs     <- getInputs
    } yield {
      val invalidOutputs = allResults.flatMap {
        case (output, results) =>
          val validInputs = results.map(_.input).toSet & inputs.toSet

          // if there are no more valid inputs, this output can be deleted
          if (validInputs.isEmpty) List(output) else List.empty
      }

      invalidOutputs.toSeq
    }
  }
}
