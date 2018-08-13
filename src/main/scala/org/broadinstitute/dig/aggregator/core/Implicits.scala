package org.broadinstitute.dig.aggregator.core

import com.amazonaws.services.s3._
import com.amazonaws.services.s3.model._
import com.amazonaws.services.elasticmapreduce._
import com.amazonaws.services.elasticmapreduce.model.StepState
import com.amazonaws.services.elasticmapreduce.model.StepSummary

import java.nio.file.Path
import java.nio.file.Paths
import java.net.URI

import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.io.Source
import scala.util._

object Implicits {

  /**
   * Helper functions for S3 objects.
   */
  implicit class RichS3Object(val s3Object: S3Object) {

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
      val contents = Source.fromInputStream(s3Object.getObjectContent).mkString

      // when done reading, be sure and close
      s3Object.close()
      contents
    }
  }

  /**
   * Helper functions for common S3 operations that are a little tricky.
   */
  implicit class RichS3Client(val s3: AmazonS3) {

    /**
     * Test whether or not a key exists.
     */
    def keyExists(bucket: String, key: String): Boolean = {
      val req = new GetObjectRequest(bucket, key)

      // ask for as little data as possible
      req.setRange(0, 0)

      // an assert indicates that the object doesn't exist
      Try(s3.getObject(req)) match {
        case Success(s3Object) => s3Object.dispose(); true
        case Failure(_)        => false
      }
    }

    /**
     * List all the keys at a given location.
     */
    def listKeys(bucket: String, key: String) = new Iterator[ObjectListing] {
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
  }

  /**
   * RichS3Client.listKeys returns one of these...
   */
  implicit class RichObjectListing(it: Iterator[ObjectListing]) {

    /**
     * Extract all the object keys from an object listing iterator.
     */
    def keys: List[String] = {
      it.flatMap(_.getObjectSummaries.asScala.map(_.getKey)).toList
    }
  }

  /**
   * When dealing with S3 paths, it's often helpful to be able to get
   * just the final filename from a path.
   */
  implicit class RichURI(uri: URI) {
    lazy val basename: String = {
      val path = Paths.get(uri.getPath)

      // extract just the final part of the path
      path.getName(path.getNameCount - 1).toString
    }
  }

  /**
   * Helper functions for a Hadoop job step.
   */
  implicit class RichStepSummary(summary: StepSummary) {
    lazy val state: StepState = StepState.valueOf(summary.getStatus.getState)

    /**
     * True if this step has successfully completed.
     */
    def isComplete: Boolean = state == StepState.COMPLETED

    /**
     * True if this step failed.
     */
    def isFailure: Boolean = state == StepState.FAILED

    /**
     * If failed, this is the reason why.
     */
    def failureReason: Option[String] = {
      Option(summary.getStatus.getFailureDetails).flatMap { details =>
        Option(details.getMessage)
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
    def stopReason: String =
      failureReason.getOrElse {
        state match {
          case StepState.INTERRUPTED => state.toString
          case StepState.CANCELLED   => state.toString
          case _                     => "Unknown"
        }
      }
  }
}
