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
abstract class DatasetProcessor(flags: Processor.Flags, config: BaseConfig) extends Processor(flags, config) {

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
    for (commit <- commits) {
      logger.info(s"Process ${commit.topic} dataset ${commit.dataset}")
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
  def run(): IO[Unit] = {
    for {
      commits <- Commit.commits(xa, topic, Some(name))
      _       <- if (flags.yes()) processCommits(commits) else showWork(commits)
    } yield ()
  }
}
