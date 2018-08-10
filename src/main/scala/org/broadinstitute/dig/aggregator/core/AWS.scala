package org.broadinstitute.dig.aggregator.core

import cats._
import cats.effect._
import cats.effect.concurrent._
import cats.implicits._

import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.regions.Regions
import com.amazonaws.services.s3._
import com.amazonaws.services.s3.model._
import com.amazonaws.services.elasticmapreduce._
import com.amazonaws.services.elasticmapreduce.model.AddJobFlowStepsRequest
import com.amazonaws.services.elasticmapreduce.model.AddJobFlowStepsResult
import com.amazonaws.services.elasticmapreduce.model.JobFlowDetail
import com.amazonaws.services.elasticmapreduce.model.ListStepsRequest
import com.amazonaws.services.elasticmapreduce.model.StepState
import com.amazonaws.services.elasticmapreduce.model.StepSummary

import com.typesafe.scalalogging.LazyLogging

import java.io.InputStream

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

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
   * Upload a file to S3 in a particular bucket.
   */
  def put(key: String, stream: InputStream): IO[PutObjectResult] = IO {
    s3.putObject(bucket, key, stream, new ObjectMetadata())
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
   * Given a set of steps, start a job.
   */
  def runJob(steps: Seq[JobStep]): IO[AddJobFlowStepsResult] = {
    val request = new AddJobFlowStepsRequest()
      .withJobFlowId(config.aws.emr.cluster)
      .withSteps(steps.map(_.config).asJava)

    IO {
      val job = emr.addJobFlowSteps(request)

      // show the job ID so it can be referenced in the AWS console
      logger.debug(s"Submitted job with ${steps.size} steps...")
      job
    }
  }

  /**
   * Every 20 seconds, send a request to the cluster to determine the state
   * of all the steps in the job. Only return once the state is one of the
   * following:
   *
   *   StepState.COMPLETED
   *   StepState.FAILED
   *   StepState.INTERRUPTED
   *   StepState.CANCELLED
   */
  def waitForJob(job: AddJobFlowStepsResult): IO[Either[StepSummary, Unit]] = {
    import Implicits._

    val request = new ListStepsRequest()
      .withClusterId(config.aws.emr.cluster)
      .withStepIds(job.getStepIds)

    // wait a little bit then request status
    val req = for (_ <- IO.sleep(20.seconds)) yield emr.listSteps(request)

    /*
     * The step summaries are returned in reverse order. The job is complete
     * when all the steps have their status set to COMPLETED, at which point
     * the curStep will be None.
     */
    val curStep = req.map(_.getSteps.asScala).map(_.find(!_.isComplete))

    /*
     * If the current step has failed, interrupted, or cancelled, then the
     * entire job is also considered failed, and return the failed step.
     */
    curStep.flatMap {
      case None => IO(Right(()))

      // the current step stopped for some reason
      case Some(step) if step.isStopped =>
        logger.error(s"Job failed: ${step.stopReason}")
        IO(Left(step))

      // still waiting for the current step to complete
      case Some(step) => {
        logger.debug(s"...${step.getName} (${step.getId}): ${step.getStatus.getState}")
        waitForJob(job)
      }
    }
  }
}
