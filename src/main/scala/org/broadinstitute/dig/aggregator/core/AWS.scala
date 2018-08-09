package org.broadinstitute.dig.aggregator.core

import scala.collection.JavaConverters._

import cats._
import cats.effect._
import cats.implicits._

import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model.DeleteObjectsRequest
import com.amazonaws.services.s3.model.DeleteObjectsResult
import com.amazonaws.services.s3.model.GetObjectRequest
import com.amazonaws.services.s3.model.ObjectListing
import com.amazonaws.services.s3.model.PutObjectResult
import com.amazonaws.services.s3.model.S3Object
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder
import com.amazonaws.services.elasticmapreduce.model.AddJobFlowStepsRequest
import com.amazonaws.services.elasticmapreduce.model.AddJobFlowStepsResult
import com.amazonaws.services.elasticmapreduce.model.HadoopJarStepConfig
import com.amazonaws.services.elasticmapreduce.model.StepConfig

import com.typesafe.scalalogging.LazyLogging

import scala.concurrent.ExecutionContext.Implicits.global

/**
 * AWS controller (S3 + EMR clients).
 */
final class AWS(config: BaseConfig) extends LazyLogging {
  private val region: Regions = Regions.valueOf(config.aws.region)
  private val bucket = config.aws.s3.bucket
  
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
   * Test whether or not a key exists.
   */
  def exists(key: String): IO[Boolean] = {
    //NB: Don't use get(), so that we can make a custom request that asks for the smallest
    //possible amount of the data (1 byte) at the key.  This is likely more efficient, and 
    //makes it easier to dispose of resources used by the returned object in a way that makes
    //the AWS SDK happy.
    val req = new GetObjectRequest(bucket, key)
    //Ask for as little data as possible, only the first byte. 
    req.setRange(0, 0)
    
    def dispose(s3Object: S3Object) = AWS.abort(s3Object) >> IO(s3Object.close())
    
    IO(s3.getObject(req)).redeemWith(
        //If anything fails, say the key doesn't exist
        recover = _ => IO(false), 
        //If get() succeeds, properly close the resulting S3Object so the SDK doesn't write loud warnings to stderr
        bind = s3Object => dispose(s3Object) >> IO(true))
  }

  /**
   * Get a list of keys within a key.
   */
  def ls(key: String, recursive: Boolean = true, pathSep: Char = AWS.pathSep): IO[List[String]] = {
    
    val notAPseudoDir = key.last != pathSep

    if (notAPseudoDir) {
      exists(key).map(keyExists => if(keyExists) List(key) else Nil)
    } else {
      // get all the immediate child keys
      val keyAndItsChildrenIo = IO {
        import Implicits._
        
        s3.listObjects(bucket, key).keys
      }      
  
      // scan all the child keys recursively
      def recurse = keyAndItsChildrenIo.flatMap { keys =>
        //Filter ourselves out, or else we loop forever!
        val childKeysOnly = keys.filter(_ != key)
        
        val rootKeyIfItExists = keys.find(_ == key)
        
        val descendantsIo: IO[List[List[String]]] = childKeysOnly.map(ls(_, true, pathSep)).sequence
        
        //flatten, and add the "root" back in if it was removed
        descendantsIo.map(_.flatten).map(rootKeyIfItExists.toList ++ _)
      }
      
      if (recursive) recurse else keyAndItsChildrenIo
    }
  }

  /**
   * Remove a list of files from S3.
   */
  def rm(keys: Seq[String]): IO[List[DeleteObjectsResult]] = {
    logger.debug(s"Deleting ${keys.size} keys")
    
    //NB: AWS will bomb out if we send a delete request with no keys.
    if(keys.isEmpty) { IO(Nil) }
    else {
      //NB: Go in chunks, since AWS can delete at most `chunkSize` objects in one request
      val chunkSize = 1000
      
      val keyChunks = keys.sliding(chunkSize, chunkSize)
      
      val keyVersionChunks = keyChunks.map(_.map(new DeleteObjectsRequest.KeyVersion(_)))
      
      val requests = keyVersionChunks.map { keyVersions => 
        (new DeleteObjectsRequest(bucket)).withKeys(keyVersions.asJava)
      }
      
      val responseIOs = requests.map(request => IO(s3.deleteObjects(request)))
      
      // the keys can be deleted in parallel
      responseIOs.toList.parSequence
    }
  }

  /**
   * Create a object to be used as a folder in S3.
   */
  def mkdir(name: String, metadata: String): IO[(PutObjectResult, PutObjectResult)] = {
    logger.debug(s"Making pseudo-dir '$name'")
    
    for {
      existing <- ls(s"$name/")
      delete <- rm(existing)
      dir <- put(s"$name/", "")
      metadata <- put(s"$name/metadata", metadata)
    } yield dir -> metadata
  }

  /**
   * Run a map/reduce job.
   */
  def runMR(jar: String, mainClass: String, args: Seq[String] = List.empty): IO[AddJobFlowStepsResult] = {
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
  
  private object Implicits {
    final implicit class RichObjectListing(val listing: ObjectListing) {
      def keys: List[String] = {
        chunksIterator.flatMap(_.getObjectSummaries.asScala.map(_.getKey)).toList
      }
      
      def chunksIterator: Iterator[ObjectListing] = Iterator(listing) ++ new Iterator[ObjectListing] {
        private[this] var listingRef = listing
        private[this] var isFirst = true
        
        override def hasNext: Boolean = isFirst || listingRef.isTruncated
  
        def next(): ObjectListing = {
          isFirst = false
          
          listingRef = s3.listNextBatchOfObjects(listingRef)
          
          listingRef
        }
      }
    }
  }
}

object AWS {
  val pathSep: Char = '/'
  
  private def abort(s3Object: S3Object): IO[Unit] = IO {
    s3Object.getObjectContent.abort()
  }
}
