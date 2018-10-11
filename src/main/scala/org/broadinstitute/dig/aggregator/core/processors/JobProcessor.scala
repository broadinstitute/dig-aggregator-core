package org.broadinstitute.dig.aggregator.core.processors

import cats._
import cats.effect._
import cats.implicits._

import com.typesafe.scalalogging.LazyLogging

import doobie._

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aggregator.core.config.BaseConfig

import scala.io.Source
import scala.io.StdIn

/**
 * An JobProcessor requires resources be uploaded to AWS and ends up running
 * job steps on the Spark cluster.
 */
abstract class JobProcessor(config: BaseConfig) extends Processor {

  /**
   * AWS client for uploading resources and running jobs.
   */
  val aws: AWS = new AWS(config.aws)

  /**
   * The collection of resources this processor needs to have uploaded
   * before the processor can run.
   */
  val resources: Seq[String]

  /**
   * Get the key location in S3 where a resource should be uploaded.
   */
  def resourceKey(resource: String) = s"resources/$resource"

  /**
   * Get the S3 URI location of a given resource that's been uploaded.
   */
  def resourceURI(resource: String) = aws.uriOf(resourceKey(resource))

  /**
   * Uploads the collection of resources to AWS if --yes was supplied on
   * the command line.
   */
  def uploadResources(flags: Processor.Flags): IO[Unit] = {
    val upload = for (res <- resources) yield {
      val source = Source.fromResource(res).mkString
      val key    = resourceKey(res)

      for {
        _ <- IO(logger.info(s"Uploading $res to S3..."))
        _ <- aws.put(key, source)
      } yield ()
    }

    if (flags.yes()) {
      upload.toList.sequence >> IO.unit
    } else {
      IO.unit
    }
  }
}
