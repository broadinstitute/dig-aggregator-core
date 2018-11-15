package org.broadinstitute.dig.aggregator.core

import cats.effect._

import fs2._

import java.net.URI
import java.nio.file.Paths

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext
import scala.io.Source
import scala.util.Try

import com.amazonaws.services.elasticmapreduce.model.StepState
import com.amazonaws.services.elasticmapreduce.model.StepSummary
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.GetObjectRequest
import com.amazonaws.services.s3.model.ObjectListing
import com.amazonaws.services.s3.model.S3Object

object Implicits {

  /**
   * Needed for IO.sleep.
   */
  implicit val timer: Timer[IO] = IO.timer(ExecutionContext.global)

  /**
   * Needed for IO.parSequence.
   */
  implicit val contextShift: ContextShift[IO] = IO.contextShift(ExecutionContext.global)

  /**
   * Helper functions for S3 objects.
   */
  final implicit class RichS3Object(val s3Object: S3Object) extends AnyVal {

    /**
     * Stop downloading and close and S3 object to free resources.
     */
    def dispose(): Unit = {
      s3Object.getObjectContent.abort()
      s3Object.close()
    }

    /**
     * Read the entire contents of an S3 object as a string.
     */
    def read(): String = {
      try {
        Source.fromInputStream(s3Object.getObjectContent).mkString
      } finally {
        dispose()
      }
    }
  }

  /**
   * Helper functions for common S3 operations that are a little tricky.
   */
  final implicit class RichS3Client(val s3: AmazonS3) extends AnyVal {

    /**
     * Test whether or not a key exists.
     */
    def keyExists(bucket: String, key: String): Boolean = {
      val req = new GetObjectRequest(bucket, key)

      // ask for as little data as possible
      req.setRange(0, 0)

      // an assert indicates that the object doesn't exist
      Try(s3.getObject(req))
        .map { s3Object =>
          s3Object.dispose()

          true
        }
        .getOrElse(false)
    }

    /**
     * List all the keys at a given location.
     */
    def listingsIterator(bucket: String, key: String): Iterator[ObjectListing] =
      new Iterator[ObjectListing] {
        private[this] var listing: Option[ObjectListing] = Some(s3.listObjects(bucket, key))

        /**
         * True if the listing was truncated and another listing can be fetched.
         */
        override def hasNext: Boolean = listing.isDefined

        /**
         * Return the current object listing and fetch the next one.
         */
        override def next(): ObjectListing = {
          val curListing = listing.get

          // fetch the next listing (if it's truncated)
          listing = curListing.isTruncated match {
            case true  => Some(s3.listNextBatchOfObjects(curListing))
            case false => None
          }

          curListing
        }
      }

    def listKeys(bucket: String, key: String): Seq[String] =
      listingsIterator(bucket, key).flatMap(_.keys).toList
  }

  /**
   * RichS3Client.listKeys returns one of these...
   */
  final implicit class RichObjectListing(val listing: ObjectListing) extends AnyVal {

    /**
     * Extract all the object keys from an object listing iterator.
     */
    def keys: Seq[String] = {
      listing.getObjectSummaries.asScala.map(_.getKey).toList
    }
  }

  /**
   * When dealing with S3 paths, it's often helpful to be able to get
   * just the final filename from a path.
   * Will return the empty string if the URI doesn't have a path part, or if the path part is empty.
   */
  final implicit class RichURI(val uri: URI) extends AnyVal {
    def basename: String = {
      val path = Paths.get(uri.getPath)

      // extract just the final part of the path
      path.iterator.asScala.toSeq.lastOption.map(_.toString).getOrElse("")
    }
  }

  /**
   * Helper functions for a Hadoop job step.
   */
  final implicit class RichStepSummary(val summary: StepSummary) extends AnyVal {
    def state: StepState = StepState.valueOf(summary.getStatus.getState)

    /**
     * True if this step matches another step and the state hasn't changed.
     */
    def matches(step: StepSummary): Boolean = {
      summary.getId == step.getId && summary.state == step.state
    }

    /**
     * True if this step has successfully completed.
     */
    def isComplete: Boolean = state == StepState.COMPLETED

    /**
     * True if this step failed.
     */
    def isFailure: Boolean = state == StepState.FAILED

    /** 
     * True if the step hasn't started yet.
     */
    def isPending: Boolean = state == StepState.PENDING

    /**
     * If failed, this is the reason why.
     */
    def failureReason: Option[String] = {
      Option(summary.getStatus).map(_.getFailureDetails).flatMap { details =>
        Option(details).flatMap(d => Option(d.getMessage))
      }
    }

    /**
     * True if this step stopped for any reason.
     */
    def isStopped: Boolean = state match {
      case StepState.FAILED      => true
      case StepState.INTERRUPTED => true
      case StepState.CANCELLED   => true
      case _                     => false
    }

    /**
     * Return a reason for why this step was stopped.
     */
    def stopReason: String = {
      failureReason.getOrElse {
        state match {
          case StepState.INTERRUPTED => state.toString
          case StepState.CANCELLED   => state.toString
          case _                     => "Unknown"
        }
      }
    }
  }
}
