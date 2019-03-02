package org.broadinstitute.dig.aggregator.core

import cats._
import cats.data._
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
import com.amazonaws.services.elasticmapreduce.model.BootstrapActionConfig
import com.amazonaws.services.elasticmapreduce.model.JobFlowDetail
import com.amazonaws.services.elasticmapreduce.model.JobFlowInstancesConfig
import com.amazonaws.services.elasticmapreduce.model.ListStepsRequest
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowRequest
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowResult
import com.amazonaws.services.elasticmapreduce.model.ScriptBootstrapActionConfig
import com.amazonaws.services.elasticmapreduce.model.StepState
import com.amazonaws.services.elasticmapreduce.model.StepSummary

import com.typesafe.scalalogging.LazyLogging

import fs2._

import java.io.InputStream
import java.net.URI

import org.broadinstitute.dig.aggregator.core.config.AWSConfig
import org.broadinstitute.dig.aggregator.core.emr.Cluster

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.io.Source
import scala.util.Random

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
    new BasicAWSCredentials(config.key, config.secret)
  )

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
   * Create a URI for a cluster log.
   */
  def logUri(cluster: Cluster): URI = uriOf(s"logs/${cluster.name}")

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

    //An IO that will produce the contents of the classpath resource at `resource` as a string,
    //and will close the InputStream backed by the resource when reading the resource's data is
    //done, either successfully or due to an error.
    val contentsIo: IO[String] = {
      val streamIo = IO(getClass.getClassLoader.getResourceAsStream(resource))

      // load the contents of the file, treat as text, ensure unix line-endings
      def getContents(stream: InputStream): IO[String] =
        IO(Source.fromInputStream(stream).mkString.replace("\r\n", "\n"))

      // close the stream to free resources
      def closeStream(stream: InputStream): IO[Unit] =
        IO(stream.close())

      // open, load, and ensure closed
      streamIo.bracket(getContents(_))(closeStream(_))
    }

    for {
      _ <- IO(logger.debug(s"Uploading $resource to S3..."))
      // load the resource in the IO context
      contents <- contentsIo
      // upload it
      _ <- put(key, contents)
    } yield {
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
  def ls(key: String, excludeSuccess: Boolean = false): IO[Seq[String]] = IO {
    val keys = s3.listKeys(bucket, key).toSeq

    // optionally filter out _SUCCESS files
    if (excludeSuccess) keys.filterNot(_.endsWith("/_SUCCESS")) else keys
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
   * Create a job request that will be used to create a new EMR cluster and
   * run a series of steps.
   */
  def runJob(cluster: Cluster, steps: Seq[JobStep]): IO[RunJobFlowResult] = {
    import Implicits.RichURI

    // create all the bootstrap actions for this cluster
    val bootstrapConfigs = cluster.bootstrapScripts.map(_.config)

    // create all the instances
    val instances = new JobFlowInstancesConfig()
      .withAdditionalMasterSecurityGroups(config.emr.securityGroupIds.map(_.value): _*)
      .withAdditionalSlaveSecurityGroups(config.emr.securityGroupIds.map(_.value): _*)
      .withEc2SubnetId(config.emr.subnetId.value)
      .withEc2KeyName(config.emr.sshKeyName.value)
      .withInstanceCount(cluster.instances)
      .withKeepJobFlowAliveWhenNoSteps(cluster.keepAliveWhenNoSteps)
      .withMasterInstanceType(cluster.masterInstanceType.value)
      .withSlaveInstanceType(cluster.slaveInstanceType.value)

    // create the request for the cluster
    val request = new RunJobFlowRequest()
      .withName(cluster.name)
      .withBootstrapActions(bootstrapConfigs.asJava)
      .withApplications(cluster.applications.map(_.application).asJava)
      .withConfigurations(cluster.configurations.map(_.configuration).asJava)
      .withReleaseLabel(config.emr.releaseLabel.value)
      .withServiceRole(config.emr.serviceRoleId.value)
      .withJobFlowRole(config.emr.jobFlowRoleId.value)
      .withAutoScalingRole(config.emr.autoScalingRoleId.value)
      .withLogUri(logUri(cluster).toString)
      .withVisibleToAllUsers(cluster.visibleToAllUsers)
      .withInstances(instances)
      .withSteps(steps.map(_.config).asJava)

    // create the IO action to launch the instance
    IO {
      val job = cluster.amiId match {
        case Some(id) => emr.runJobFlow(request.withCustomAmiId(id.value))
        case None     => emr.runJobFlow(request)
      }

      // show the cluster, job ID and # of total steps being executed
      logger.info(s"Starting ${cluster.name} as ${job.getJobFlowId} with ${steps.size} steps")

      // return the job
      job
    }
  }

  /**
   * Helper: create a job that's a single step.
   */
  def runJob(cluster: Cluster, step: JobStep): IO[RunJobFlowResult] = {
    runJob(cluster, Seq(step))
  }

  /**
   * Every few minutes, send a request to the cluster to determine the state
   * of all the steps in the job. Only return once the state is one of the
   * following for the last/current step:
   *
   *   StepState.COMPLETED
   *   StepState.FAILED
   *   StepState.INTERRUPTED
   *   StepState.CANCELLED
   */
  def waitForJob(job: RunJobFlowResult, prevStep: Option[StepSummary] = None): IO[RunJobFlowResult] = {
    import Implicits.timer

    // create the job request
    val request = new ListStepsRequest()
      .withClusterId(job.getJobFlowId)

    // wait a little bit then request status
    val req = for (_ <- IO.sleep(1.minutes)) yield emr.listSteps(request)

    // get the total number of steps so progress can be shown
    val numSteps = req.map(_.getSteps.asScala.size)

    /*
     * The step summaries are returned in reverse order. The job is complete
     * when all the steps have their status set to COMPLETED, at which point
     * the curStep will be None.
     */
    val curStep: IO[Option[(StepSummary, Int, Int)]] =
      for {
        n    <- req.map(_.getSteps.asScala.size)
        step <- req.map(_.getSteps.asScala.reverse.zipWithIndex).map(_.find(!_._1.isComplete))
      } yield {
        step.map {
          case (summary, index) => (summary, index, n)
        }
      }

    /*
     * If the current step has failed, interrupted, or cancelled, then the
     * entire job is also considered failed, and return the failed step.
     */
    curStep.flatMap {
      case None =>
        logger.debug(s"Job ${job.getJobFlowId} complete")
        IO(job)

      // the current step stopped for some reason
      case Some((step, i, n)) if step.isStopped =>
        logger.error(
          s"Job ${job.getJobFlowId} failed: ${step.stopReason}; logs in S3 and visible from the EMR web console."
        )

        // terminate the program
        IO.raiseError(new Exception(step.stopReason))

      // hasn't started yet; cluster is still provisioning, continue waiting
      case Some((step, _, _)) if step.isPending =>
        waitForJob(job, Some(step))

      // still waiting for the current step to complete
      case Some((step, i, n)) =>
        val changed = prevStep match {
          case Some(prev) => !step.matches(prev)
          case None       => true
        }

        if (changed) {
          val jar  = step.getConfig.getJar
          val args = step.getConfig.getArgs.asScala.mkString(" ")

          // show progress and debugging information
          logger.info(s"...${job.getJobFlowId} ${step.getStatus.getState} step ${i + 1}/$n")
          logger.debug(s"...${job.getJobFlowId} ${step.getStatus.getState}: $jar $args (${step.getId})")
        }

        // continue waiting
        waitForJob(job, Some(step))
    }
  }

  /**
   * Given a sequence of jobs, run them in parallel, but limit the maximum
   * concurrency so too many clusters aren't created at once.
   */
  def waitForJobs(jobs: Seq[IO[RunJobFlowResult]], maxClusters: Int = 5): IO[Unit] = {
    Utils.waitForTasks(jobs, maxClusters) { job =>
      job.flatMap(waitForJob(_))
    }
  }

  /**
   * Often times there are N jobs that are all identical (aside from command
   * line parameters) that need to be run, and can be run in parallel.
   *
   * This can be done by spinning up a unique cluster for each, but has the
   * downside that the provisioning step (which can take several minutes) is
   * run for each job.
   *
   * This function allows a list of "jobs" (read: a list of a list of steps)
   * to be passed, and N clusters will be made that will run through all the
   * jobs until complete. This way the provisioning costs are only paid for
   * once.
   *
   * This should only be used if all the jobs can be run in parallel.
   *
   * NOTE: The jobs are shuffled so that jobs that may be large and clumped
   *       together won't happen every time the jobs run together.
   */
  def clusterJobs(cluster: Cluster, jobs: Seq[Seq[JobStep]], maxClusters: Int = 5): Seq[IO[RunJobFlowResult]] = {
    val indexedJobs = Random.shuffle(jobs).zipWithIndex.map {
      case (job, i) => (i % maxClusters, job)
    }

    // round-robin each job into a cluster
    val clusteredJobs = indexedJobs.groupBy(_._1).mapValues(_.map(_._2))

    // for each cluster, create a "job" that's all the steps appended
    clusteredJobs.values
      .map(jobs => runJob(cluster, jobs.flatten))
      .toSeq
  }
}
