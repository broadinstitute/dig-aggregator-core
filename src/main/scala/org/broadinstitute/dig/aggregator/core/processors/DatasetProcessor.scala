package org.broadinstitute.dig.aggregator.core.processors

import cats.effect.IO

import org.broadinstitute.dig.aggregator.core._

/**
 * A DatasetProcessor is a CommitProcessor that - for each dataset committed -
 * processes the dataset. This is processor is treated specially for 2 reasons:
 *
 *  1. The --reset flag means that existing datasets already committed to the
 *     database need to have their commits reprocessed.
 *
 *  2. After each dataset is processed, they need to be written to the database
 *     so that a future --reset won't (necessarily) reprocess them and so that
 *     there is a log of what processes have been run on which datasets for
 *     the end-user.
 *
 * The `topic` parameter is the source of the commit records to filter on. For
 * example: the "variants" topic. Any commit record not from this topic will
 * be ignored.
 */
abstract class DatasetProcessor(opts: Opts, sourceTopic: String)
    extends CommitProcessor(opts, Some(sourceTopic)) {

  /**
   * If --reset was passed on the command line, then farm out to the database
   * and query the commits table for a record of all datasets already committed
   * for this topic that need to be processed by this application.
   */
  val oldCommits: IO[Seq[Commit]] = {
    if (opts.reprocess()) { Commit.datasetCommits(xa, sourceTopic, opts.ignoreProcessedBy) } else {
      IO.pure(Nil)
    }
  }

  /**
   * First process all the old commits, then start reading from Kafka.
   */
  override def run(): IO[Unit] = {
    /*
     * TODO: It would be much safer if assigning the partitions and getting the
     *       old commits were part of the same transaction. As it's coded now,
     *       it's possible for the commits table to be updated immediately
     *       after being queried, but immediately before the existing commits
     *       are fetched, meaning that the new commits will be processed, and
     *       then when the consumer begins those commits will be processed
     *       again.
     *
     *       This is a rare case to fix and technically shouldn't hurt anything
     *       but should be avoided if possible. There are two ways to solve it:
     *
     *       1. Get the state and commits in the same Transactor:
     *
     *          val transaction = for {
     *            stateQuery       <- consumer.assignPartitions()
     *            commitsQuery     <- Commit.datasets(sourceTopic)
     *          } yield (state, commits)
     *
     *          transaction.transact(xa)
     *
     *       2. Get the state and then use the state to modify the datasets
     *          query with a set of SQL fragments that limit the commit offset
     *          allowed:
     *
     *          for {
     *            state   <- consumer.assignPartitions()
     *            commits <- Commit.datasets(sourceTopic, state)
     *            _       <- ...
     *          } yield ()
     *
     *       Either of these solutions will require a decent amount of work.
     */

    for {
      state   <- getState
      commits <- oldCommits
      _       <- processCommits(commits)
      _       <- consumer.consume(state, processRecords)
    } yield ()
  }
}
