package org.broadinstitute.dig.aggregator.core.processors

import cats.effect._

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aggregator.core.config.BaseConfig

/** A DatasetProcessor is a Processor that queries the `datasets` table for
  * what datasets have been written to HDFS and have not yet been processed.
  *
  * DatasetProcessors are always the entry point to a pipeline.
  */
abstract class DatasetProcessor(name: Processor.Name, config: BaseConfig) extends Processor[Dataset](name) {

  /** All topic committed datasets come from.
    */
  val topic: String

  /** Database transactor for loading state, etc.
    */
  protected val pool: DbPool = DbPool.fromMySQLConfig(config.mysql)

  /** AWS client for uploading resources and running jobs.
    */
  protected val aws: AWS = new AWS(config.aws)

  /** Process a set of committed datasets.
    */
  def processDatasets(datasets: Seq[Dataset]): IO[Unit]

  /** Calculates the set of things this processor needs to process.
    */
  override def getWork(opts: Processor.Opts): IO[Seq[Dataset]] = {
    for {
      datasets <- Dataset.datasetsOf(pool, topic, if (opts.reprocess) None else Some(name))
    } yield {
      datasets
        .filter(d => opts.onlyGlob.matches(d.dataset))
        .filterNot(d => opts.excludeGlob.matches(d.dataset))
    }
  }

  /** Determine the list of datasets that need processing, process them, and
    * write to the database that they were processed.
    */
  override def run(opts: Processor.Opts): IO[Unit] = {
    for {
      work <- getWork(opts)
      _    <- uploadResources(aws)
      _    <- if (opts.insertRuns) IO.unit else processDatasets(work)
      _    <- insertRuns(pool, work)
    } yield ()
  }
}
