package org.broadinstitute.dig.aggregator.core.processors

import cats._
import cats.effect._
import cats.implicits._

import com.typesafe.scalalogging.LazyLogging

import doobie._

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aggregator.core.config.BaseConfig

import scala.io.StdIn

/**
 * An IntakeProcessor is resposible for listening to datasets being uploaded on
 * a given topic and writing them to HDFS.
 */
abstract class IntakeProcessor(config: BaseConfig) extends Processor {

  /**
   * The topic this processor is consuming from.
   */
  val topic: String

  /**
   * Database transactor for loading state, etc.
   */
  protected val xa: Transactor[IO] = config.mysql.newTransactor()

  /**
   * Subclass responsibility.
   */
  def processRecords(records: Seq[Consumer.Record]): IO[_]

  /**
   * IO to load this consumer's state from the database.
   */
  def loadState: IO[State] = {
    State.load(xa, name, topic)
  }

  /**
   * IO to create the reset state for this consumer.
   */
  def resetState: IO[State] = {
    State.reset(xa, name, topic)
  }

  /**
   * Either load the state from the database or reset the state back to a
   * known, good offset and continue consuming from there.
   */
  def getState(reprocess: Boolean): IO[State] = {
    if (reprocess) {
      val warning = IO {
        logger.warn("The consumer state is being reset because the --reprocess")
        logger.warn("flag was passed on the command line.")
        logger.warn("")
        logger.warn("If this is the desired course of action, answer 'Y' at")
        logger.warn("the prompt; any other response will exit the program")
        logger.warn("before any damage is done.")
        logger.warn("")

        StdIn.readLine("[y/N]: ").equalsIgnoreCase("y")
      }

      // terminate the entire application if the user doesn't answer "Y"
      warning.flatMap { confirm =>
        if (confirm) resetState else IO.raiseError(new Exception("reset canceled"))
      }
    } else {
      loadState
    }
  }

  /**
   * Create a new consumer and start consuming records from Kafka.
   */
  def run(flags: Processor.Flags): IO[Unit] = {
    val consumer = new Consumer(config.kafka, topic, xa)

    for {
      loadedState <- getState(flags.reprocess())
      state       <- consumer.assignPartitions(loadedState)

      /*
       * At this point, the consumer has logged where it would begin consuming
       * from: topic, partitions, and offsets. Unless --yes was supplied on
       * the command line, we should not actually process anything!
       */

      // consume messages or just show successful connection
      _ <- if (flags.yes()) consumer.consume(state, processRecords) else IO.unit
    } yield ()
  }
}
