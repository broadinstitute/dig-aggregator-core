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
  def processDatasets(commits: Seq[Dataset]): IO[_]

  /**
   * Output results that would be processed if the --yes flag was specified.
   */
  def showWork(datasets: Seq[Dataset]): IO[_] = IO {
    datasets.size match {
      case 0 => logger.info(s"Everything up to date.")
      case _ =>
        for (dataset <- datasets) {
          logger.info(s"Process dataset '${dataset.dataset}' for topic '${dataset.topic}'")
        }
    }
  }

  /**
   * Determine the list of datasets that need processing, process them, write
   * to the database that they were processed, and send a message to the
   * analyses topic.
   *
   * When this processor is running in "process" mode (consuming from Kafka),
   * this is called whenever the analyses topic has a message sent to it.
   *
   * Otherwise, this is just called once and then exits.
   */
  override def run(flags: Processor.Flags): IO[Unit] = {
    val notProcessedBy = if (flags.reprocess()) None else Some(name)

    // optionally upload all the resources for this processor
    val uploads = resources.map { resource =>
      if (flags.yes()) aws.upload(resource) else IO.unit
    }

    for {
      _ <- uploads.toList.sequence

      // fetch the list of datasets for this topic not yet processed
      datasets <- Dataset.datasetsOf(xa, topic, notProcessedBy)

      // either process them or show what would be processed
      _ <- if (flags.yes()) processDatasets(datasets) else showWork(datasets)
    } yield ()
  }
}
