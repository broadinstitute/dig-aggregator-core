package org.broadinstitute.dig.aggregator.core

import cats.effect._
import cats.implicits._

import com.typesafe.scalalogging.LazyLogging

import doobie._
import doobie.implicits._

import java.util.UUID

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
      uuid: UUID,
      processor: Processor.Name,
      input: Option[UUID],
      output: String,
      repo: String,
      branch: String,
      commit: String
  )

  /** Implicit conversion to/from DB string from/to UUID for doobie.
    */
  implicit val nameGet: Get[UUID] = Get[String].tmap(UUID.fromString)
  implicit val namePut: Put[UUID] = Put[String].tcontramap(_.toString)

  /** Run entries are created and inserted atomically for a single output.
    */
  def insert(pool: DbPool, processor: Processor.Name, output: String, inputs: Option[Seq[UUID]]): IO[UUID] = {
    val q = s"""|INSERT INTO `runs`
                |  ( `uuid`
                |  , `processor`
                |  , `input`
                |  , `output`
                |  , `repo`
                |  , `branch`
                |  , `commit`
                |  )
                |
                |VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                |
                |ON DUPLICATE KEY UPDATE
                |  `run` = VALUES(`run`),
                |  `repo` = VALUES(`repo`),
                |  `branch` = VALUES(`branch`),
                |  `commit` = VALUES(`commit`),
                |  `timestamp` = NOW()
                |""".stripMargin

    // generate the run ID and an insert-multi update
    val uuid = UUID.randomUUID
    val prov = Provenance.thisBuild

    // create an entry per input or a single entry with no input
    val entries = inputs match {
      case None => Seq(Entry(uuid, processor, None, output, prov.source, prov.branch, prov.commit))
      case Some(ids) =>
        ids.map { uuid =>
          Entry(uuid, processor, Some(uuid), output, prov.source, prov.branch, prov.commit)
        }
    }

    // insert a row per entry
    val insert = Update[Entry](q).updateMany(entries.toList)

    for {
      _ <- IO(logger.debug(s"Inserting run output '$output' for $processor..."))
      _ <- pool.exec(insert)
    } yield uuid
  }

  /** Given a UUID, delete all run results for it.
    */
  def deleteRun(pool: DbPool, uuid: UUID): IO[Unit] = {
    pool.exec(sql"DELETE FROM `runs` WHERE `uuid`=$uuid".update.run).as(())
  }

  /** Given a processor name, delete all runs created by it.
    */
  def deleteRuns(pool: DbPool, processor: Processor.Name): IO[Unit] = {
    pool.exec(sql"DELETE FROM `runs` WHERE `processor`=$processor".update.run).as(())
  }

  /** When querying the `runs` table to determine what has already been
    * processed by dependency applications, this is what is returned.
    */
  final case class Result(uuid: UUID, processor: Processor.Name, output: String, timestamp: java.time.Instant) {
    override def toString: String = s"$processor:$output"
  }

  /** Lookup the runs of a given processor.
    */
  def runsOfProcessor(pool: DbPool, processor: Processor.Name): IO[Seq[UUID]] = {
    pool.exec(sql"SELECT DISTINCT `uuid` FROM `runs` WHERE `processor`=$processor".query[UUID].to[Seq])
  }

  /** Lookup all the inputs of a given run.
    */
  def inputsOfRun(pool: DbPool, uuid: UUID): IO[Seq[UUID]] = {
    val q = sql"SELECT `input` FROM `runs` WHERE `uuid`=$uuid"

    pool
      .exec(q.query[Option[UUID]].to[Seq])
      .map(_.flatten)
  }

  /** Lookup all the results for a given run id. This is mostly used for testing.
    */
  def resultsOfRun(pool: DbPool, uuid: UUID): IO[Seq[Result]] = {
    val q = sql"""|SELECT `uuid`, `processor`, `output`, `timestamp`
                  |FROM   `runs`
                  |WHERE  `uuid`=$uuid
                  |""".stripMargin.query[Result].to[Seq]

    pool.exec(q)
  }

  /** Build a SQL fragment that looks up the run results of a set of processors.
    */
  def resultsOfFragment(processors: Seq[Processor.Name]): Fragment = {
    val selects = processors.map { processor =>
      fr"SELECT `uuid`, `processor`, `output`, `timestamp` FROM `runs` WHERE `processor`=$processor"
    }

    // union all the runs produced by all the processors together
    val union = selects.toList.intercalate(fr"UNION ALL")
    val group = fr"GROUP BY `processor`, `output`"

    // union all the outputs together and group them
    union ++ group
  }

  /** Find all the run results processed by a set of processors.
    */
  def resultsOf(pool: DbPool, processors: Seq[Processor.Name]): IO[Seq[Result]] = {
    pool.exec(resultsOfFragment(processors).query[Result].to[Seq])
  }

  /** Find all the run results processed by a set of processors, but NOT
    * yet processed by another.
    */
  def resultsOf(pool: DbPool, processors: Seq[Processor.Name], notProcessedBy: Processor.Name): IO[Seq[Result]] = {
    val inputs = resultsOfFragment(processors)

    // use the inputs as a subquery
    val select = fr"SELECT `inputs`.`uuid`, `inputs`.`processor`, `inputs`.`output`, `inputs`.`timestamp`"
    val from   = fr"FROM (" ++ inputs ++ fr") AS `inputs`"

    // join with the runs that used the inputs
    val join = fr"""|LEFT OUTER JOIN `runs` AS `r`
                    |ON `r`.`processor` = $notProcessedBy
                    |AND `r`.`input` = inputs.`uuid`
                    |AND `r`.`timestamp` > inputs.`timestamp`
                    |""".stripMargin

    // filter runs where the join failed (read: was not processed)
    val where = fr"WHERE `r`.`processor` IS NULL"

    // run the query
    pool.exec((select ++ from ++ join ++ where).query[Result].to[Seq])
  }

  /** Verify the inputs of a single run by asserting that all the inputs exist.
    * Returns a list of invalid input runs (including the one passed in) if
    * invalid.
    */
  def verifyRun(pool: DbPool, run: UUID): IO[Seq[UUID]] = {
    for {
      inputs <- inputsOfRun(pool, run)

      // recursively test the inputs for whether they are invalid
      invalidInputs <- inputs
        .map(uuid => verifyRun(pool, uuid))
        .toList
        .sequence
    } yield {
      if (invalidInputs.isEmpty) Seq.empty else Seq(run) ++ invalidInputs.flatten
    }
  }

  /** Verify the results of a processor by asserting that all its inputs actually
    * exist. This must be done recursively for each input. If an input doesn't exist
    * any more then the output of the run is invalid and needs to be deleted.
    */
  def verifyRuns(pool: DbPool, processor: Processor.Name): IO[Seq[UUID]] = {
    for {
      runs        <- runsOfProcessor(pool, processor)
      invalidRuns <- runs.map(verifyRun(pool, _)).toList.sequence
    } yield invalidRuns.flatten
  }
}
