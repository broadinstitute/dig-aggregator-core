package org.broadinstitute.dig.aggregator.core

import cats._
import cats.effect._
import cats.implicits._

import com.typesafe.scalalogging.Logger

import scala.io.StdIn

/**
 * A Processor will consume records from a given topic and call a process
 * function to-be-implemented by a subclass.
 */
abstract class Processor(opts: Opts, val topic: String) {

  /**
   * Application logger for this processor.
   */
  val logger = Logger(opts.appName)

  /**
   * Database transactor for loading state, etc.
   */
  protected val xa = opts.config.mysql.newTransactor()

  /**
   * The Kafka topic consumer.
   */
  protected val consumer = new Consumer(opts, topic)

  /**
   * Subclass responsibility.
   */
  def processRecords(records: Seq[Consumer.Record]): IO[_]

  /**
   * IO to load this consumer's state from the database.
   */
  def loadState: IO[State] = {
    State.load(xa, opts.appName, topic)
  }

  /**
   * IO to create the reset state for this consumer.
   */
  def resetState: IO[State] = {
    State.reset(xa, opts.appName, topic)
  }

  /**
   * Determine if the state is being reset (with --reset) or loaded from the
   * database and return the correct operation to execute.
   */
  def getState: IO[State] = {
    if (opts.reset()) {
      val warning = IO {
        logger.warn("The consumer state is being reset because either reset")
        logger.warn("flag was passed on the command line or the commits")
        logger.warn("database doesn't contain any partition offsets for this")
        logger.warn("application + topic.")
        logger.warn("")
        logger.warn("If this is the desired course of action, answer 'Y' at")
        logger.warn("the prompt; any other response will exit the program")
        logger.warn("before any damage is done.")
        logger.warn("")

        StdIn.readLine("[y/N]: ").equalsIgnoreCase("y")
      }

      // terminate the entire application if the user doesn't answer "Y"
      warning.flatMap { confirm =>
        if (confirm) resetState else IO.raiseError(new Exception("state reset canceled"))
      }
    } else {
      loadState
    }
  }

  /**
   * Create a new consumer and start consuming records from Kafka.
   */
  def run(): IO[Unit] = {
    for {
      state <- getState
      _     <- consumer.consume(state, processRecords)
    } yield ()
  }
}

/**
 * A specific Processor that processes commit records. Mostly just a helper
 * that parses a commit record before processing.
 */
abstract class CommitProcessor(opts: Opts, sourceTopic: Option[String])
    extends Processor(opts, "commits") {

  /**
   * Subclass resposibility.
   */
  def processCommits(commits: Seq[Commit]): IO[_]

  /**
   * Parse each record as a Commit and process the commits.
   */
  def processRecords(records: Seq[Consumer.Record]): IO[_] = {
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
  val oldCommits = {
    if (opts.reset()) Commit.datasets(xa, sourceTopic) else IO.pure(Nil)
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
