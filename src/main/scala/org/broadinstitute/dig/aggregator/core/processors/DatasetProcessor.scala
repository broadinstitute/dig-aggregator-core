package org.broadinstitute.dig.aggregator.core.processors

import cats._
import cats.effect._
import cats.implicits._

import doobie._

import com.typesafe.scalalogging.LazyLogging

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aggregator.core.config.BaseConfig

/**
 * A DatasetProcessor is a Processor that queries the `datasets` table for
 * what datasets have been written to HDFS and have not yet been processed.
 *
 * DatasetProcessors are always the entry point to a pipeline.
 */
abstract class DatasetProcessor(name: Processor.Name, config: BaseConfig) extends Processor(name) {
  import Implicits.contextShift

  /**
   * All topic committed datasets come from.
   */
  val topic: String

  /**
   * The collection of resources this processor needs to have uploaded
   * before the processor can run.
   */
  val resources: Seq[String]

  /**
   * Database transactor for loading state, etc.
   */
  protected val xa: Transactor[IO] = config.mysql.newTransactor()

  /**
   * AWS client for uploading resources and running jobs.
   */
  protected val aws: AWS = new AWS(config.aws)

  /**
   * Process a set of committed datasets.
   */
  def processDatasets(commits: Seq[Dataset]): IO[Unit]

  /**
   * Calculates the set of things this processor needs to process.
   */
  override def getWork(reprocess: Boolean, only: Option[String]): IO[Seq[Dataset]] = {
    for {
      datasets <- Dataset.datasetsOf(xa, topic, if (reprocess) None else Some(name))
    } yield {
      datasets.filter(d => only.getOrElse(d.dataset) == d.dataset)
    }
  }

  /**
   * Determine the list of datasets that need processing, process them, and
   * write to the database that they were processed.
   */
  override def run(reprocess: Boolean, only: Option[String]): IO[Unit] = {
    for {
      _ <- resources.map(aws.upload(_)).toList.sequence
      _ <- getWork(reprocess, only).flatMap(processDatasets)
    } yield ()
  }
}
