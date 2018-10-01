package org.broadinstitute.dig.aggregator.core.processors

import cats.effect.IO

import com.typesafe.scalalogging.LazyLogging

import doobie.util.transactor.Transactor

import org.broadinstitute.dig.aggregator.core._

import scala.io.StdIn

/**
 * A Processor will consume records from a given topic and call a process
 * function to-be-implemented by a subclass.
 */
abstract class Processor(opts: Opts, val topic: String) extends LazyLogging {

  /**
   * Database transactor for loading state, etc.
   */
  protected val xa: Transactor[IO] = opts.config.mysql.newTransactor()

  /**
   * The Kafka topic consumer.
   */
  protected val consumer: Consumer = new Consumer(opts, topic)

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
    if (opts.reprocess()) {
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
