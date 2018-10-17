package org.broadinstitute.dig.aggregator.core.processors

import cats._
import cats.effect._
import cats.implicits._

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aggregator.core.config.BaseConfig

import org.json4s._
import org.json4s.JsonDSL.WithBigDecimal._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization.write

/**
 * An UploadProcessor is resposible for writing entire datasets being read from
 * Kafka to HDFS using the same protocol across all data type:
 *
 *  CREATE - clears old data and creates the dataset directory structure
 *  UPLOAD - writes a variants batch to the dataset directory
 *  COMMIT - resend a commit message to the commits topic in Kafka
 *
 * TODO: add DELETE and possibly other methods...
 *
 * The data written to HDFS will be done as:
 *
 *  hdfs://<topic>/<id>/metadata
 *  hdfs://<topic>/<id>/data-<offset>
 */
abstract class UploadProcessor[A <: AnyRef](name: Processor.Name, config: BaseConfig)(implicit m: Manifest[A])
    extends IntakeProcessor(name, config) {
  implicit val formats: Formats = DefaultFormats

  /**
   * Datasets are written to AWS (S3).
   */
  protected val aws: AWS = new AWS(config.aws)

  /**
   * When a dataset is complete, send a message to the commits topic.
   */
  private val commitsProducer = new Producer(config.kafka, "commits")

  /**
   * Subclass responsibility.
   */
  override def processRecords(records: Seq[Consumer.Record]): IO[_] = {
    val ios = for (record <- records) yield {
      val json = parse(record.value)

      // dataset unique ID - NOT NECESSARILY THE SAME AS record.key!
      val id     = (json \ "id").extract[String]
      val method = (json \ "method").extract[String]
      val body   = (json \ "body")

      // perform task based on method
      method match {
        case "CREATE" => createDataset(record, id, body)
        case "UPLOAD" => uploadDataset(record, id, body)
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
  protected def logRecord(record: Consumer.Record, msg: String) = IO {
    logger.info(s"(${record.partition},${record.offset}) - $msg")
  }

  /**
   * Erases any dataset already in HDFS with the same ID and creates a new
   * directory ready to accept incoming data.
   */
  private def createDataset(record: Consumer.Record, id: String, body: JValue): IO[Unit] = {
    val metadata = compact(render(body.extract[JObject]))

    for {
      _ <- aws.mkdir(s"$topic/$id", metadata)
      _ <- logRecord(record, s"Created $topic/$id/ dataset directory")
    } yield ()
  }

  /**
   * Upload a batch of data to a file in HDFS for this dataset.
   */
  private def uploadDataset(record: Consumer.Record, id: String, body: JValue): IO[Unit] = {
    val count = (body \ "count").extract[Int]
    val data  = (body \ "data").extract[Seq[A]]

    // make sure the data matches
    require(count == data.size, s"Incorrect count ($count != ${data.size})")

    // convert the data to a compact string, 1-line per entry
    val contents = data
      .map(entry => write(entry))
      .mkString("\n")

    // where to write the file to; ensure a unique name using the offset
    val file = s"$topic/$id/data-${record.offset}"

    for {
      _ <- aws.put(file, contents)
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
      _ <- commitsProducer.send(id, message)
      _ <- logRecord(record, s"Commited dataset '$id'")
    } yield ()
  }
}
