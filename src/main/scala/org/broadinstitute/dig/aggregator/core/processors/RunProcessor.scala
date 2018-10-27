package org.broadinstitute.dig.aggregator.core.processors

import cats._
import cats.effect._
import cats.implicits._

import doobie._

import com.typesafe.scalalogging.LazyLogging

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aggregator.core.config.BaseConfig

/**
 * A RunProcessor is a Processor that queries the `runs` table to determine
 * what outputs have been produced by applications it depends on, which set of
 * those it hasn't processed yet, and process them.
 */
abstract class RunProcessor(name: Processor.Name, config: BaseConfig) extends Processor(name) {

  /**
   * All the processors this processor depends on.
   */
  val dependencies: Seq[Processor.Name]

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
   * Process a set of run results. Must return the output location where this
   * process produced data.
   */
  def processResults(results: Seq[Run.Result]): IO[_]

  /**
   * Output results that would be processed if the --yes flag was specified.
   */
  def showWork(results: Seq[Run.Result]): IO[_] = IO {
    if (results.isEmpty) {
      logger.info(s"Everything up to date.")
    } else {
      for (result <- results) {
        logger.info(s"Process output of ${result.app}: ${result.output}")
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

      // get the results not yet processed
      results <- Run.resultsOf(xa, dependencies, notProcessedBy)

      // either process them or show what would be processed
      _ <- if (flags.yes()) processResults(results) else showWork(results)
    } yield ()
  }
}
