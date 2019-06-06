package org.broadinstitute.dig.aggregator.core.processors

import cats.effect._

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aggregator.core.config.BaseConfig

/** A RunProcessor is a Processor that queries the `runs` table to determine
  * what outputs have been produced by applications it depends on, which set of
  * those it hasn't processed yet, and process them.
  */
abstract class RunProcessor(name: Processor.Name, config: BaseConfig) extends Processor[Run.Result](name) {

  /** All the processors this processor depends on.
    */
  val dependencies: Seq[Processor.Name]

  /** Database transactor for loading state, etc.
    */
  protected val pool: DbPool = DbPool.fromMySQLConfig(config.mysql)

  /** AWS client for uploading resources and running jobs.
    */
  protected val aws: AWS = new AWS(config.aws)

  /** Process a set of run results. Must return the output location where this
    * process produced data.
    */
  def processResults(results: Seq[Run.Result]): IO[_]

  /** Calculates the set of things this processor needs to process.
    */
  override def getWork(opts: Processor.Opts): IO[Seq[Run.Result]] = {
    for {
      results <- Run.resultsOf(pool, dependencies, if (opts.reprocess) None else Some(name))
    } yield {
      results
        .filter(r => opts.onlyGlobs.exists(_.matches(r.output)))
        .filterNot(r => opts.excludeGlobs.exists(_.matches(r.output)))
    }
  }

  /** Determine the list of datasets that need processing, process them, write
    * to the database that they were processed, and send a message to the
    * analyses topic.
    *
    * When this processor is running in "process" mode (consuming from Kafka),
    * this is called whenever the analyses topic has a message sent to it.
    *
    * Otherwise, this is just called once and then exits.
    */
  override def run(opts: Processor.Opts): IO[Unit] = {
    for {
      work <- getWork(opts)
      _    <- uploadResources(aws)

      // if only inserting runs, skip processing
      _ <- if (opts.insertRuns) IO.unit else processResults(work)
      _ <- insertRuns(pool, work)
    } yield ()
  }
}
