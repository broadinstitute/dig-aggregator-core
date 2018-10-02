package org.broadinstitute.dig.aggregator.core.processors

import cats._
import cats.effect._
import cats.implicits._

import org.broadinstitute.dig.aggregator.core._

/**
 * A RunProcessor is a Processor that queries the `runs` table to determine
 * what outputs have been produced by applications it depends on, which set of
 * those it hasn't processed yet, and process them.
 *
 * It can be launched with one of two methods:
 *
 *  `run`     - like any other processor, it waits for messages from Kafka on
 *              the `analyses` topic and - upon receiving them - determines the
 *              set of outputs to be processed and processes them
 *
 *  `runOnce` - skips reading from Kafka, determines the set of outputs that
 *              need to be processed, does so, and then exits
 */
abstract class RunProcessor(opts: Opts, deps: Seq[String]) extends Processor(opts, "analyses") {

  /**
   * Whenever this processor runs, it will send what work was done to the
   * analyses topic.
   */
  val analyses: Producer = new Producer(opts, "analyses")

  /**
   * Process a set of run results. Must return the output location where this
   * process produced data.
   */
  def processInputs(inputs: Seq[Run.Result]): IO[String]

  /**
   * When listening to Kafka for messages indicating that data has been
   * processed, this will consume all those messages, check to see if any
   * of them come from a dependency application, and then run if so.
   */
  override def processRecords(records: Seq[Consumer.Record]): IO[Unit] = {
    runOnce
  }

  /**
   * Run processors should always start consuming from the end of the topic
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
    for {
      inputs <- Run.runs(xa, opts.appName, deps)
      output <- processInputs(inputs)
      _      <- writeRun(inputs, output)

      // TODO: send to analyses topic, maybe write a run/log entry?
    } yield ()
  }

  /**
   * Ensure that all the datasets are written to the database with the updated
   * application name that processed them.
   */
  protected def writeRun(input: Seq[Run.Result], output: String): IO[Unit] = {
    val id = System.currentTimeMillis()

    // create a run entry for every input
    val runs = input.map { input =>
      Run(id, opts.appName, input.output, output)
    }

    // write them all to the database
    runs.map(_.insert(xa)).toList.sequence >> IO.unit
  }
}
