package org.broadinstitute.dig.aggregator.core.processors

import cats._
import cats.effect._
import cats.implicits._

import org.broadinstitute.dig.aggregator.core._

import org.json4s._
import org.json4s.JsonDSL.WithBigDecimal._
import org.json4s.jackson.JsonMethods._

/**
 * After a dataset has been completely processed, from then on it's the
 * AnalysisProcessor that take over.
 *
 * At this point, whatever work needs to be done to the global data sitting
 * in HDFS is dependent on previous work having been done (as opposed to the
 * data existing at all).
 *
 * All of these processors listen to the same topic: `analyses`. This topic is
 * a single partition (like `commits`), and consists of simple messages that
 * indicate what analysis was complete and what data - unique to each - is
 * associated with the work that was done. Each AnalysisProcessor is expected
 * to filter off the previous, reduce the data, run its own analysis, and then
 * post its own message back to the topic so any processor downstream of itself
 * can run.
 *
 * The template parameters:
 *
 *   I - the input type from the previous analysis message
 *   O - the output type produced by this processor (input to the next)
 */
abstract class AnalysisProcessor[I, O](opts: Opts, dep: String, typ: Analysis.Type)(implicit m: Manifest[I],
                                                                                    ev1: O => JValue)
    extends Processor(opts, "analyses") {

  // After processing our work, send a message back to the same topic.
  private val producer: Producer = new Producer(opts, "analyses")

  /**
   * All the input messages from a consumer record batch are processed together
   * and produce a single output. It is intended that this processor is
   * capable of reducing the inputs to do as little work as possible.
   *
   * If each of the inputs must be processed serially, then the output is
   * expected to be the aggregation of all the inputs being processed.
   *
   * The type returned from this is the input to the next analysis processor
   * that is waiting on this one.
   *
   * Subclass responsibility.
   */
  def processAnalyses(input: Seq[Analysis[I]]): IO[O]

  /**
   * Analysis processors always reset their state starting from the end of
   * the `analyses` topic. It is expected that they get a list of all the
   * work that needs to be redone and perform it as this will likely be
   * unique per processor.
   */
  override def resetState: IO[State] = {
    State.fromEnd(opts.appName, consumer)
  }

  /**
   * Parse each record as an I, reduce them together, then run the job and
   * send the result back to the `jobs` topic for the next JobProcessor to
   * consume and run with.
   */
  override def processRecords(records: Seq[Consumer.Record]): IO[_] = {
    implicit val formats = DefaultFormats

    // parse all the record values as JSON
    val input = records
      .map(Analysis.fromRecord[I])
      .filter(analysis => analysis.app == dep)

    // for each input, process the analysis
    for {
      out <- processAnalyses(input)

      // write the output back to the analyses topic
      write = Analysis(opts.appName, typ, out).send(producer)

      // write the output back to the jobs topic for the next job
      _ <- if (opts.noWrite()) IO.unit else (write >> IO.unit)
    } yield ()
  }
}
