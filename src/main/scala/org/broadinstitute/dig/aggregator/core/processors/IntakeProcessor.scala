package org.broadinstitute.dig.aggregator.core.processors

import cats._
import cats.effect._
import cats.implicits._

import doobie._

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aggregator.core.config.BaseConfig

import scala.io.StdIn

/**
 * An IntakeProcessor is resposible for listening to datasets being uploaded on
 * a given topic and writing them to HDFS.
 */
abstract class IntakeProcessor(name: Processor.Name, config: BaseConfig) extends Processor(name) {

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
   * Create a new consumer and start consuming records from Kafka.
   */
  override def run(flags: Processor.Flags): IO[Unit] = {
    val consumer = new Consumer(config.kafka, topic, xa)

    for {
      loadedState <- if (flags.reprocess()) resetState else loadState
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
