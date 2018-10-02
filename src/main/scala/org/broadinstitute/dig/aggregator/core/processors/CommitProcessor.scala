package org.broadinstitute.dig.aggregator.core.processors

import cats.effect.IO

import com.typesafe.scalalogging.LazyLogging

import doobie.util.transactor.Transactor

import org.broadinstitute.dig.aggregator.core._

import scala.io.StdIn

/**
 * A specific Processor that processes commit records. Mostly just a helper
 * that parses a commit record before processing.
 */
abstract class CommitProcessor(opts: Opts, sourceTopic: Option[String]) extends Processor(opts, "commits") {

  /**
   * Subclass resposibility.
   */
  def processCommits(commits: Seq[Commit]): IO[_]

  /**
   * Parse each record as a Commit and process the commits.
   */
  override def processRecords(records: Seq[Consumer.Record]): IO[_] = {
    val commits = records.map(Commit.fromRecord)

    // optionally filter commits by source topic
    val filteredCommits = sourceTopic match {
      case Some(topic) => commits.filter(_.topic == topic)
      case None        => commits
    }

    // process all the commit records
    processCommits(filteredCommits)
  }

  /**
   * Commit processors only ever care about the last commit offset and not
   * the offsets of the source topic.
   */
  override def resetState: IO[State] = {
    State.lastCommit(xa, opts.appName, sourceTopic)
  }
}
