package org.broadinstitute.dig.aggregator.core

import cats._
import cats.effect._
import cats.implicits._

/**
 * A Processor will consume records from a given topic and call a process
 * function to-be-implemented by a subclass.
 */
abstract class Processor(opts: Opts, topic: String) {

  /**
   * Subclass resposibility.
   */
  def processRecords(records: Seq[Consumer.Record]): IO[_]

  /**
   * Create a new consumer and start consuming records from Kafka.
   */
  def run(): IO[Unit] = {
    val consumer = new Consumer(opts, topic)

    for {
      state <- consumer.assignPartitions()
      _     <- consumer.consume(state, processRecords)
    } yield ()
  }
}

/**
 * A specific Processor that processes commit records. Mostly just a helper
 * that parses a commit record before processing.
 */
abstract class CommitProcessor(opts: Opts) extends Processor(opts, "commits") {

  /**
   * Subclass resposibility.
   */
  def processCommits(commits: Seq[Commit]): IO[_]

  /**
   * Parse each record as a Commit and process the commits.
   */
  def processRecords(records: Seq[Consumer.Record]): IO[_] = {
    processCommits(records.map(Commit.fromRecord))
  }
}

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
abstract class DatasetProcessor(opts: Opts, topic: String) extends CommitProcessor(opts) {

  /**
   * Database transactor for writing dataset rows and fetching commits.
   */
  private val xa = opts.config.mysql.newTransactor()

  /**
   * If --reset was passed on the command line, then farm out to the database
   * and query the commits table for a record of all datasets already committed
   * for this topic that need to be processed by this application.
   */
  val oldCommits = {
    if (opts.reset()) Commit.datasets(xa, topic) else IO.pure(Nil)
  }

  /**
   * After processing the records, log the processing of the dataset(s) to
   * the database.
   */
  override def processRecords(records: Seq[Consumer.Record]): IO[Unit] = {
    super.processRecords(records) >> IO.unit // TODO: <-- insert datasets table
  }

  /**
   * First process all the old commits, then start reading from Kafka.
   */
  override def run(): IO[Unit] = {
    val consumer = new Consumer(opts, topic)

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
     *            commitsQuery     <- Commit.datasets(topic)
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
     *            commits <- Commit.datasets(topic, state)
     *            _       <- ...
     *          } yield ()
     *
     *       Either of these solutions will require a decent amount of work.
     */

    for {
      state   <- consumer.assignPartitions() // first because of reset warning!
      commits <- oldCommits
      _       <- processCommits(commits)
      _       <- consumer.consume(state, processRecords)
    } yield ()
  }
}
