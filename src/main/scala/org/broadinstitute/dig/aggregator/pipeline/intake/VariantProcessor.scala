package org.broadinstitute.dig.aggregator.pipeline.intake

import cats.effect._
import cats.implicits._

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aggregator.core.config.BaseConfig
import org.broadinstitute.dig.aggregator.core.processors._

import org.json4s._
import org.json4s.JsonDSL.WithBigDecimal._
import org.json4s.jackson.JsonMethods._

import scala.collection.JavaConverters._
import scala.util._
import com.typesafe.scalalogging.LazyLogging

/**
 * Handles three method types:
 *
 *  CREATE - clears old data and creates the dataset directory structure
 *  UPLOAD - writes a variants batch to the dataset directory
 *  COMMIT - resend a commit message to the commits topic in Kafka
 */
class VariantProcessor(flags: Processor.Flags, config: BaseConfig) extends IntakeProcessor(flags, config) {
  private implicit val formats: Formats = DefaultFormats

  /**
   * Unique name identifying this processor.
   */
  val name: Processor.Name = Processors.variantProcessor

  /**
   * Topic to consume.
   */
  val topic: String = "variants"

  /**
   * When a dataset is complete, send a message to the commits topic.
   */
  private val producer: Producer = new Producer(config.kafka, "commits")

  /**
   * Datasets are written to S3.
   */
  private val cluster: AWS = new AWS(config.aws)

  /**
   * Process each of the records from Kafka based on the method.
   */
  override def processRecords(records: Seq[Consumer.Record]): IO[Unit] = {
    val ios = for (record <- records) yield {
      val json = parse(record.value)

      // dataset unique ID - NOT NECESSARILY THE SAME AS record.key!
      val id     = (json \ "id").extract[String]
      val method = (json \ "method").extract[String]
      val body   = (json \ "body")

      // perform task based on method
      method match {
        case "CREATE" => createDataset(record, id, body)
        case "UPLOAD" => uploadVariants(record, id, body)
        case "COMMIT" => commitDataset(record, id)
        case _        => IO.raiseError(new Exception(s"Unknown METHOD: $method"))
      }
    }

    // wait for them to all complete
    ios.toList.sequence >> IO.unit
  }

  /**
   * Log a message for a particular partition/offset.
   */
  private def logRecord(record: Consumer.Record, msg: String) = IO {
    logger.info(s"(${record.partition},${record.offset}) - $msg")
  }

  /**
   * Create a new folder in the S3 bucket for this dataset.
   */
  private def createDataset(record: Consumer.Record, id: String, body: JValue): IO[Unit] = {
    val metadata = compact(render(body.extract[JObject]))
    val topic    = record.topic

    for {
      _ <- cluster.mkdir(s"$topic/$id", metadata)
      _ <- logRecord(record, s"Created $topic/$id/ dataset directory")
    } yield ()
  }

  /**
   * Upload a batch of variants to a file in the S3 bucket for this dataset.
   */
  private def uploadVariants(record: Consumer.Record, id: String, body: JValue): IO[Unit] = {
    val count    = (body \ "count").extract[Int]
    val variants = (body \ "variants").extract[List[JObject]]

    // convert the variants to a compact string, 1-line per variant
    val contents = variants
      .map(json => compact(render(json)))
      .mkString("\n")

    // where to write the file to; ensure a unique name using the offset
    val file = s"${record.topic}/$id/variants-${record.offset}"

    for {
      _ <- cluster.put(file, contents)
      _ <- logRecord(record, s"$count variants written to '$file'")
    } yield ()
  }

  /**
   * Commit the dataset by resending the commit message back to Kafka on
   * the commits topic for other processors to get and do something with.
   */
  private def commitDataset(record: Consumer.Record, id: String): IO[Unit] = {
    val message = Commit.message(record, id)

    // forward to the commit topic for aggregators
    for {
      _ <- producer.send(id, message)
      _ <- logRecord(record, s"Commited dataset '$id'")
    } yield ()
  }
}
