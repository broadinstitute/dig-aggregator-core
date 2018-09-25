package org.broadinstitute.dig.aggregator.core

import cats._
import cats.effect._
import cats.implicits._

import org.apache.kafka.clients.producer._

import org.broadinstitute.dig.aggregator.core._

import org.json4s._
import org.json4s.JsonDSL.WithBigDecimal._
import org.json4s.jackson.JsonMethods._

/**
 * Generic analysis that can be written correctly to the `analyses` topic.
 */
case class Analysis[A](app: String, `type`: Analysis.Type, data: A)(implicit ev1: A => JValue) {
  import Analysis.type2json

  /**
   * The JSON object representation of this analysis. This is a function so
   * json4s doesn't attempt to serialize it.
   */
  def json: JObject = ("app" -> app) ~ ("type" -> `type`) ~ ("data" -> data)

  /**
   * Send the analysis to the Kafka topic.
   */
  def send(producer: Producer) = {
    require(producer.topic == "analyses", "Cannot sent analyis to topic ${producer.topic}")

    // send the message using the application name as the key
    producer.send(app, compact(render(json)))
  }
}

/**
 * Companion object for deserializing analyses.
 */
object Analysis {
  import scala.language.implicitConversions

  /**
   * All analyses are one of a couple types.
   */
  sealed trait Type

  /**
   * Analysis types.
   */
  final case object Experiment  extends Type
  final case object Computation extends Type

  /**
   * Implicit conversion from analysis type to JSON value.
   */
  implicit def type2json(typ: Type): JValue = JString(typ.toString)

  /**
   * Read a record from the analyses topic and return the analysis of the
   * expected type for it.
   */
  def fromRecord[A](record: Consumer.Record)(implicit m: Manifest[A]): Analysis[A] = {
    implicit val formats = DefaultFormats
    parse(record.value).extract[Analysis[A]]
  }
}
