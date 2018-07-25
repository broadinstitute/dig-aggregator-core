package org.broadinstitute.dig.aggregator.core

import cats._
import cats.effect._
import cats.implicits._

import com.amazonaws.auth._
import com.amazonaws.regions._
import com.amazonaws.services.elasticmapreduce._
import com.amazonaws.services.elasticmapreduce.model.{ Unit => _, _ }
import com.amazonaws.services.elasticmapreduce.util._
import com.amazonaws.services.s3._
import com.amazonaws.services.s3.model._

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.collection.mutable.Buffer
import scala.collection.mutable.ListBuffer

/**
 * AWS controller (S3 + EMR clients).
 */
final class AWS[C <: BaseConfig](opts: Opts[C]) {
  private val region: Regions = Regions.valueOf(opts.config.aws.region)

  /**
   * AWS IAM credentials provider.
   */
  private val credentials: AWSStaticCredentialsProvider = new AWSStaticCredentialsProvider(
    new BasicAWSCredentials(opts.config.aws.key, opts.config.aws.secret))

  /**
   * S3 client for storage.
   */
  private val s3: AmazonS3 = AmazonS3ClientBuilder.standard
    .withRegion(region)
    .withCredentials(credentials)
    .build

  /**
   * EMR client for running map/reduce jobs.
   */
  private val emr: AmazonElasticMapReduce = AmazonElasticMapReduceClientBuilder.standard
    .withCredentials(credentials)
    .withRegion(region)
    .build

  /**
   * Upload a string to S3 in a particular bucket.
   */
  def put(key: String, text: String): IO[PutObjectResult] = IO {
    s3.putObject(opts.config.aws.s3.bucket, key, text)
  }

  /**
   * Download a file from an S3 bucket.
   */
  def get(key: String): IO[S3Object] = IO {
    s3.getObject(opts.config.aws.s3.bucket, key)
  }

  private def keysFrom(listing: ObjectListing): List[String] = {
    val keys: Buffer[String] = new ListBuffer

    def getKeys(l: ObjectListing): Iterable[String] = l.getObjectSummaries.asScala.map(_.getKey)

    keys ++= getKeys(listing)

    // might be broken up into multiple requests
    while (listing.isTruncated) {
      val next = s3.listNextBatchOfObjects(listing)

      // update the list of keys
      keys ++= getKeys(next)
    }

    keys.toList
  }

  /**
   * Get a list of keys within a key.
   */
  def ls(key: String, recursive: Boolean = true, pathSep: Char = '/'): IO[List[String]] = {
    if (key.last != pathSep) {
      return IO(List(key))
    }

    // get all the immediate child keys
    val io = IO {
      val listing = s3.listObjects(opts.config.aws.s3.bucket, key)
      var keys = listing.getObjectSummaries.asScala.map(_.getKey).toList

      // might be broken up into multiple requests
      while (listing.isTruncated) {
        val next = s3.listNextBatchOfObjects(listing)

        // update the list of keys
        keys ++= next.getObjectSummaries.asScala.map(_.getKey).toList
      }

      keys
    }

    // immediate children only
    if (!recursive) {
      return io
    }

    // scan all the child keys recursively
    io.flatMap(_.map(ls(_, true, pathSep)).sequence).map(_.flatten)
  }

  /**
   * Remove a list of files from S3.
   */
  def rm(keys: Seq[String]): IO[Unit] = IO {
    if (keys.nonEmpty) {
      val objs = keys.map(new DeleteObjectsRequest.KeyVersion(_))
      val request = (new DeleteObjectsRequest(opts.config.aws.s3.bucket)).withKeys(objs.asJava)

      s3.deleteObjects(request)
    }
  }

  /**
   * Create a object to be used as a folder in S3.
   */
  def mkdir(name: String, metadata: String): IO[PutObjectResult] = {
    for {
      existing <- ls(s"$name/")
      delete <- rm(existing)
      dir <- put(s"$name/", "")
      metadata <- put(s"$name/_metadata", metadata)
    } yield metadata
  }

  /**
   * Run a map/reduce job.
   */
  def runMR(jar: String, mainClass: String, args: Seq[String] = List.empty): IO[AddJobFlowStepsResult] = {
    val config = new HadoopJarStepConfig()
      .withJar(s"s3://${opts.config.aws.s3.bucket}/jobs/$jar")
      .withMainClass(mainClass)
      .withArgs(args.asJava)

    // create the step to run this config
    val step = new StepConfig(mainClass, config)

    // create the request to run the step
    val request = new AddJobFlowStepsRequest()
      .withJobFlowId(opts.config.aws.emr.cluster)

    // start it
    IO {
      emr.addJobFlowSteps(request)
    }
  }
}
