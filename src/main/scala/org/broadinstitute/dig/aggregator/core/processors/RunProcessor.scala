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
abstract class RunProcessor(config: BaseConfig) extends JobProcessor(config) {

  /**
   * All the processors this processor depends on.
   */
  val dependencies: Seq[Processor.Name]

  /**
   * Database transactor for loading state, etc.
   */
  val xa: Transactor[IO] = config.mysql.newTransactor()

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

    for {
      _ <- uploadResources(flags)

      // get the results not yet processed
      results <- Run.resultsOf(xa, dependencies, notProcessedBy)

      // either process them or show what would be processed
      _ <- if (flags.yes()) processResults(results) else showWork(results)
    } yield ()
  }
}
