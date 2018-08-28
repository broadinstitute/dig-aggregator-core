package org.broadinstitute.dig.aggregator.core.app

import cats._
import cats.effect._
import cats.implicits._

import org.broadinstitute.dig.aggregator.core._

import org.json4s._
import org.json4s.jackson.JsonMethods._

/**
 * A DigDatasetProcessorApp consumes from the `commits` topic in Kafka and -
 * for each dataset committed - runs a process function.
 *
 * Additionally, when `--reset` is passed on the command line, before consuming
 * from Kafka, a dataset processor will go back and re-process datasets that
 * were previously committed, but may not have been processed by this app.
 */
abstract class DigDatasetProcessorApp(topic: String) extends DigApp {

  /**
   * Process all the records in a batch of commits consumed from Kafka.
   */
  def processCommits(commits: Seq[Commit]): IO[Unit]

  /**
   * Filter commits not from the topic this processor cares about.
   */
  private def processRecords(commits: Seq[Commit]): IO[Unit] = {
    processCommits(commits.filter(_.topic == topic))
  }

  /**
   * Entry point.
   */
  def run(opts: Opts): IO[ExitCode] = {
    val consumer = new Consumer[Commit](opts, "commits")(Commit.fromCommitRecord)

    // if --reset was passed then re-process datasets previously committed
    val processReset = if (opts.reset()) {
      // TODO: Check for flag to force reprocessing of already processed datasets.

      // fetch committed datasets from the database for this topic and process
      Commit.datasets(consumer.xa, topic) >>= processCommits
    } else {
      IO.unit
    }

    for {
      state <- consumer.assignPartitions()
      _     <- processReset
      _     <- consumer.consume(state, processRecords)
    } yield ExitCode.Success
  }
}
