package org.broadinstitute.dig.aggregator.core

import scala.collection.JavaConverters._

import cats._
import cats.effect._
import cats.implicits._

import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.regions.Regions
import com.amazonaws.services.s3._
import com.amazonaws.services.s3.model._
import com.amazonaws.services.elasticmapreduce._
import com.amazonaws.services.elasticmapreduce.model.AddJobFlowStepsRequest
import com.amazonaws.services.elasticmapreduce.model.AddJobFlowStepsResult
import com.amazonaws.services.elasticmapreduce.model.HadoopJarStepConfig
import com.amazonaws.services.elasticmapreduce.model.StepConfig

import com.typesafe.scalalogging.LazyLogging

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

/**
 * AWS controller (S3 + EMR clients).
 */
final class AWS(config: BaseConfig) extends LazyLogging {
  import Implicits._

  /**
   * The same region and bucket are used for all operations.
   */
  private val region: Regions = Regions.valueOf(config.aws.region)
  private val bucket          = config.aws.s3.bucket

  /**
   * AWS IAM credentials provider.
   */
  private val credentials: AWSStaticCredentialsProvider = new AWSStaticCredentialsProvider(
    new BasicAWSCredentials(config.aws.key, config.aws.secret))

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
   * Test whether or not a key exists.
   */
  def exists(key: String): IO[Boolean] = IO {
    s3.keyExists(bucket, key)
  }

  /**
   * Upload a string to S3 in a particular bucket.
   */
  def put(key: String, text: String): IO[PutObjectResult] = IO {
    s3.putObject(bucket, key, text)
  }

  /**
   * Download a file from an S3 bucket.
   */
  def get(key: String): IO[S3Object] = IO {
    s3.getObject(bucket, key)
  }

  /**
   * Delete a key from S3.
   */
  def rm(key: String): IO[Unit] = IO {
    s3.deleteObject(bucket, key)
  }

  /**
   * Delete (recursively) all the keys under a given key from S3.
   */
  def rmdir(key: String): IO[List[String]] = {
    val ios = for (listing <- s3.listKeys(bucket, key)) yield {
      val keys        = listing.getObjectSummaries.asScala.map(_.getKey)
      val keyVersions = keys.map(new DeleteObjectsRequest.KeyVersion(_))
      val request     = new DeleteObjectsRequest(bucket).withKeys(keyVersions.asJava)

      for {
        _ <- IO(logger.debug(s"Deleting ${keys.head} + ${keys.tail.size} more keys..."))
        _ <- IO(s3.deleteObjects(request))
      } yield keys
    }

    // all the delete operations can happen in parallel
    ios.toList.parSequence.map(_.foldLeft(List.empty[String])(_ ++ _))
  }

  /**
   * Create a object to be used as a folder in S3.
   */
  def mkdir(key: String, metadata: String): IO[PutObjectResult] = {
    logger.debug(s"Creating pseudo-dir '$key'")

    for {
      keysDeleted <- rmdir(key)
      metadata    <- put(s"$key/metadata", metadata)
    } yield metadata
  }

  /**
   * Run a map/reduce job.
   */
  def runMR(jar: String,
            mainClass: String,
            args: Seq[String] = List.empty): IO[AddJobFlowStepsResult] = {
    val stepConfig = new HadoopJarStepConfig()
      .withJar(s"s3://${bucket}/jobs/$jar")
      .withMainClass(mainClass)
      .withArgs(args.asJava)

    // create the step to run this config
    val step = new StepConfig(mainClass, stepConfig)

    // create the request to run the step
    val request = new AddJobFlowStepsRequest()
      .withJobFlowId(config.aws.emr.cluster)

    // start it
    IO {
      emr.addJobFlowSteps(request)
    }
  }
}
