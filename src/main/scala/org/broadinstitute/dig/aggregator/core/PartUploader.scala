package org.broadinstitute.dig.aggregator.core

import cats._
import cats.effect._
import cats.implicits._

import com.amazonaws.services.s3.model._

import org.json4s._
import org.json4s.JsonDSL.WithBigDecimal._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization.write

import scala.collection.mutable.ArrayBuffer

/**
 *
 */
class PartUploader[A <: Topic](aws: AWS, val dataset: Dataset)(implicit m: Manifest[A]) {
  private val batch = new ArrayBuffer[A]()
  private val i     = Iterator.from(1)
  private val path  = s"${dataset.topic}/${dataset.dataset}"

  /**
   * Before adding any items to the uploader, first prepare the dataset
   * folder by deleting everything already there.
   */
  def prepare(metadata: JObject): IO[PutObjectResult] = {
    implicit val formats = DefaultFormats

    aws.mkdir(path, compact(render(metadata)))
  }

  /**
   * When a batch gets too large or we're completely done adding items to
   * the batch, commit what's there and upload it to HDFS.
   */
  def commit(): IO[Unit] = {
    implicit val formats = DefaultFormats

    if (batch.size == 0) {
      IO.unit
    } else {
      val contents = batch
        .map(write(_))
        .mkString("\n")

      for {
        _ <- aws.put(s"$path/part-${i.next}", contents)
      } yield batch.clear()
    }
  }

  /**
   * Add a new entry to the batch. If the batch is large enough, flush it by
   * writing the file to HDFS.
   */
  def +=(entry: A): IO[Unit] = {
    batch += entry

    if (batch.size < 1000000) {
      IO.unit
    } else {
      commit()
    }
  }
}
