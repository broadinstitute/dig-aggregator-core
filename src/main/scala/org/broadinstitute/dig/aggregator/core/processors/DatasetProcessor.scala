package org.broadinstitute.dig.aggregator.core.processors

import cats._
import cats.effect._
import cats.implicits._

import org.broadinstitute.dig.aggregator.core._

/**
 * A DatasetProcessor is a Processor that can discern the set of datasets that
 * are new and it hasn't processed yet, process them, and update the database
 * that it has done that work.
 *
 * Each DatasetProcessor can have an optional application that it is dependent
 * on (`dep`).
 *
 * If it does, then it will look for all the datasets that its dependency has
 * processed, but it has not yet processed to determine the set of work that it
 * has to do.
 *
 * If it does NOT have a dependency, then the set of work needing to be done
 * is the datasets committed for the given source `topic` that have not yet
 * been processed by this processor.
 *
 * When a DatasetProcessor is created, it can be launched with one of two
 * methods:
 *
 *  `run`     - like any other processor, it waits for messages from Kafka on
 *              the analyses topic and - upon receiving them - determines the
 *              set of datasets that it needs to process and processes them
 *
 *  `runOnce` - skips reading from Kafka, determines the set of datasets that
 *              need to be processed, does so, and then exits
 */
abstract class DatasetProcessor[A](opts: Opts, topic: String, dep: Option[String]) extends Processor(opts, "analyses") {

  /**
   * Whenever this processor runs, it will send what work was done to the
   * analyses topic.
   */
  val analyses: Producer = new Producer(opts, "analyses")

  /**
   * Process a series of datasets that have yet to be processed by this app.
   *
   * Note: it is possible for the same dataset to be represented multiple times
   *       within th `datasets` argument.
   */
  def processDatasets[A](datasets: Seq[Dataset]): IO[A]

  /**
   * The `analyses` is just for dataset processors to send a quick message
   * indicating that they have done work. They are not intended to carry any
   * data about the work to be used by downstream processors.
   *
   * For this reason, when a dataset processor gets a message that work has
   * been done (by anyone), they can simply go to the database, check to see
   * what work it needs to do, and do it.
   */
  override def processRecords(records: Seq[Consumer.Record]): IO[Unit] = {
    runOnce
  }

  /**
   * Dataset processors should always start consuming from the end of the topic
   * since they are transient messages. For this reason, the processor should
   * run once, then start consuming.
   *
   * Note: it may take a while to runOnce the first time. For this reason, it
   *       is best to get the state first, as messages may come in while data
   *       is being processed indicating that it should run again.
   */
  override def run(): IO[Unit] = {
    for {
      state <- State.fromEnd(opts.appName, consumer)
      _     <- runOnce
      _     <- consumer.consume(state, processRecords)
    } yield ()
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
  def runOnce(): IO[Unit] = {
    val datasetsToProcess: IO[Seq[Dataset]] =
      (dep, opts.ignoreProcessedBy) match {

        // dependency and not yet processed by this app
        case (Some(dep), Some(app)) =>
          Dataset.datasets(xa, dep, topic, app)

        // dependency and reprocess all of them
        case (Some(dep), None) =>
          Dataset.datasets(xa, dep, topic)

        // no dependency and not yet processed by this app
        case (None, Some(app)) =>
          Commit.commits(xa, topic, app).map(_.map(Dataset.fromCommit))

        // no dependency and reprocess all of them
        case (None, None) =>
          Commit.commits(xa, topic).map(_.map(Dataset.fromCommit))
      }

    for {
      datasets <- datasetsToProcess
      data     <- processDatasets(datasets)
      _        <- writeDatasets(datasets)

      // TODO: send to analyses topic, maybe write a run entry?
    } yield ()
  }

  /**
   * Ensure that all the datasets are written to the database with the updated
   * application name that processed them.
   */
  protected def writeDatasets(datasets: Seq[Dataset]): IO[Unit] = {
    val writes = datasets
      .map(_.copy(app = Some(opts.appName)))
      .map(_.insert(xa))
      .toList
      .sequence

    writes >> IO.unit
  }
}
