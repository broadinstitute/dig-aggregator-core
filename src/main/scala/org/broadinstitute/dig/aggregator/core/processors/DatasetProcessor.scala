package org.broadinstitute.dig.aggregator.core.processors

import cats._
import cats.effect._
import cats.implicits._

import doobie._

import com.typesafe.scalalogging.LazyLogging

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aggregator.core.config.BaseConfig

/**
 * A DatasetProcessor is a Processor that queries the `commits` table for
 * what datasets have been written to HDFS and have not yet been processed.
 *
 * DatasetProcessors are always the entry point to a pipeline.
 */
abstract class DatasetProcessor(config: BaseConfig) extends JobProcessor(config) {

  /**
   * All topic committed datasets come from.
   */
  val topic: String

  /**
   * Database transactor for loading state, etc.
   */
  val xa: Transactor[IO] = config.mysql.newTransactor()

  /**
   * Process a set of committed datasets.
   */
  def processCommits(commits: Seq[Commit]): IO[_]

  /**
   * Output results that would be processed if the --yes flag was specified.
   */
  def showWork(commits: Seq[Commit]): IO[_] = IO {
    commits.size match {
      case 0 => logger.info(s"Everything up to date.")
      case _ =>
        for (commit <- commits) {
          logger.info(s"Process dataset '${commit.dataset}' for topic '${commit.topic}'")
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
  def run(flags: Processor.Flags): IO[Unit] = {
    val notProcessedBy = if (flags.reprocess()) None else Some(name)

    for {
      _ <- uploadResources(flags)

      // fetch the list of datasets for this topic not yet processed
      commits <- Commit.commits(xa, topic, notProcessedBy)

      // either process them or show what would be processed
      _ <- if (flags.yes()) processCommits(commits) else showWork(commits)
    } yield ()
  }
}
