package org.broadinstitute.dig.aggregator.core.app

import cats._
import cats.effect._

import org.broadinstitute.dig.aggregator.core._

import org.json4s._
import org.json4s.jackson.JsonMethods._

/**
 * A DigProcessorApp will consume records from a given topic in Kafka, parse
 * the JSON as a record of type `A` and call the `processRecords` function.
 */
abstract class DigProcessorApp[A](topic: String)(implicit m: Manifest[A]) extends DigApp {
  implicit val formats = DefaultFormats

  /**
   * Process all the records in a batch consumed from kafka.
   */
  def processRecords(records: Seq[A]): IO[Unit]

  /**
   * Entry point of application.
   */
  def run(opts: Opts): IO[ExitCode] = {
    val consumer = new Consumer[A](opts, topic)(record => parse(record.value).extract[A])

    for {
      state <- consumer.assignPartitions()
      _     <- consumer.consume(state, processRecords)
    } yield ExitCode.Success
  }
}
