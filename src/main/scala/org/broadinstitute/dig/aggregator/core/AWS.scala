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
import java.net.URI

import org.broadinstitute.dig.aggregator.core.config.AWSConfig

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.io.Source

/**
 * AWS controller (S3 + EMR clients).
 */
final class AWS(config: AWSConfig) extends LazyLogging {
  import Implicits._

  /**
   * The same region and bucket are used for all operations.
   */
  val region: Regions = Regions.valueOf(config.region)
  val bucket: String  = config.s3.bucket

  /**
   * AWS IAM credentials provider.
   */
  val credentials: AWSStaticCredentialsProvider = new AWSStaticCredentialsProvider(
    new BasicAWSCredentials(config.key, config.secret))

  /**
   * S3 client for storage.
   */
  val s3: AmazonS3 = AmazonS3ClientBuilder.standard
    .withRegion(region)
    .withCredentials(credentials)
    .build

  /**
   * EMR client for running map/reduce jobs.
   */
  val emr: AmazonElasticMapReduce = AmazonElasticMapReduceClientBuilder.standard
    .withCredentials(credentials)
    .withRegion(region)
    .build

  /**
   * Returns the URI to a given key.
   */
  def uriOf(key: String): URI = new URI(s"s3://$bucket/$key")

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
   * Upload a resource file to S3 (using a matching key) and return a URI to it.
   */
  def upload(resource: String, dirKey: String = "resources"): IO[URI] = {
    val key = s"""$dirKey/${resource.stripPrefix("/")}"""

    for {
      _ <- IO(logger.debug(s"Uploading $resource to S3..."))

      // load the resource in the IO context
      stream = getClass.getResourceAsStream(resource)
      source = Source.fromInputStream(stream).mkString

      // upload it
      _ <- put(key, source)
    } yield {
      stream.close()
      uriOf(key)
    }
  }

  /**
   * Fetch a file from an S3 bucket (does not download content).
   */
  def get(key: String): IO[S3Object] = IO {
    s3.getObject(bucket, key)
  }

  /**
   * Returns the canonical URL for a given key.
   */
  def publicUrlOf(key: String): String = {
    s3.getUrl(bucket, key).toExternalForm
  }

  /**
   * Delete a key from S3.
   */
  def rm(key: String): IO[Unit] = IO {
    s3.deleteObject(bucket, key)
  }

  /**
   * List all the keys in a given S3 folder.
   */
  def ls(key: String): IO[Seq[String]] = IO {
    s3.listKeys(bucket, key).toSeq
  }

  /**
   * Delete (recursively) all the keys under a given key from S3.
   */
  def rmdir(key: String): IO[Seq[String]] = {
    val ios = for (listing <- s3.listingsIterator(bucket, key)) yield {
      if (listing.getObjectSummaries.isEmpty) {
        IO(Nil)
      } else {
        val keys        = listing.keys
        val keyVersions = keys.map(new DeleteObjectsRequest.KeyVersion(_))
        val request     = new DeleteObjectsRequest(bucket).withKeys(keyVersions.toSeq.asJava)

        for {
          _ <- IO(logger.debug(s"Deleting ${keys.head} + ${keys.tail.size} more keys..."))
          _ <- IO(s3.deleteObjects(request))
        } yield keys
      }
    }

    // all the delete operations can happen in parallel
    ios.toList.parSequence.map(_.flatten)
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
      .withJobFlowId(config.emr.cluster)
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
   * following for the last/current step:
   *
   *   StepState.COMPLETED
   *   StepState.FAILED
   *   StepState.INTERRUPTED
   *   StepState.CANCELLED
   */
  def waitForJob(job: AddJobFlowStepsResult, prevStep: Option[StepSummary] = None): IO[Unit] = {
    import Implicits._

    val request = new ListStepsRequest()
      .withClusterId(config.emr.cluster)
      .withStepIds(job.getStepIds)

    // wait a little bit then request status
    val req = for (_ <- IO.sleep(5.seconds)) yield emr.listSteps(request)

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
      case None =>
        logger.debug(s"Job complete")
        IO.unit

      // the current step stopped for some reason
      case Some(step) if step.isStopped =>
        logger.error(s"Job failed: ${step.stopReason}")
        logger.error(s"View output logs from the master node of the cluster by running:")
        logger.error(s"  yarn logs --applicationId <id>")
        IO.fromEither(Left(new Throwable(step.stopReason)))

      // still waiting for the current step to complete
      case Some(step) =>
        val changed = prevStep match {
          case Some(prev) => !step.matches(prev)
          case None       => true
        }

        // if the step/state has changed, log the change
        if (changed) {
          logger.debug(s"...${step.getName} (${step.getId}): ${step.getStatus.getState}")
        }

        // continue waiting
        waitForJob(job, Some(step))
    }
  }

  /**
   * Helper: runs a job and waits for the results to complete.
   */
  def runJobAndWait(steps: Seq[JobStep]): IO[Unit] = {
    runJob(steps).flatMap(waitForJob(_))
  }

  /**
   * Helper: run a single step as a job.
   */
  def runStep(step: JobStep): IO[AddJobFlowStepsResult] = {
    runJob(List(step))
  }

  /**
   * Helper: run a single step and wait for it to complete.
   */
  def runStepAndWait(step: JobStep): IO[Unit] = {
    runStep(step).flatMap(waitForJob(_))
  }
}
