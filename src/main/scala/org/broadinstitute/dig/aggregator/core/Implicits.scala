package org.broadinstitute.dig.aggregator.core

import com.amazonaws.services.s3._
import com.amazonaws.services.s3.model._
import com.amazonaws.services.elasticmapreduce._

import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util._

/**
 *
 */
object Implicits {

  /**
   * Helper functions for common S3 operations that are a little tricky.
   */
  implicit class RichS3Client(val s3: AmazonS3) {

    /**
     * Stop downloading and close and S3 object to free resources.
     */
    def dispose(s3Object: S3Object): Unit = {
      s3Object.getObjectContent.abort()
      s3Object.close()
    }

    /**
     * Test whether or not a key exists.
     */
    def keyExists(bucket: String, key: String): Boolean = {
      val req = new GetObjectRequest(bucket, key)

      // ask for as little data as possible
      req.setRange(0, 0)

      // an assert indicates that the object doesn't exist
      Try(s3.getObject(req)) match {
        case Success(s3Object) => dispose(s3Object); true
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
   *
   */
  implicit class RichObjectListing(it: Iterator[ObjectListing]) {

    /**
     * Extract all the object keys from an object listing iterator.
     */
    def keys: List[String] = {
      it.flatMap(_.getObjectSummaries.asScala.map(_.getKey)).toList
    }
  }
}
