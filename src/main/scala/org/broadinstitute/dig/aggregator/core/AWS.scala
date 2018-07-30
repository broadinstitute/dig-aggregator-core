package org.broadinstitute.dig.aggregator.core

import scala.collection.JavaConverters._

import cats.effect.IO
import cats.implicits._

import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model.DeleteObjectsRequest
import com.amazonaws.services.s3.model.DeleteObjectsResult
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

/**
 * AWS controller (S3 + EMR clients).
 */
final class AWS[C <: BaseConfig](opts: Opts[C]) extends LazyLogging {
  
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
    def getKeys(l: ObjectListing): Iterable[String] = l.getObjectSummaries.asScala.map(_.getKey)
    
    import Implicits._
    
    listing.iterator.flatMap(getKeys).toList
  }

  /**
   * Get a list of keys within a key.
   */
  def ls(key: String, recursive: Boolean = true, pathSep: Char = AWS.pathSep): IO[List[String]] = {
    logger.debug(s"Listing (${if(!recursive) "NOT " else ""}recursively) keys under '$key'")
    
    val notAPseudoDir = key.last != pathSep
    
    if (notAPseudoDir) {
      //TODO: Should we go out to AWS in this case?  Doesn't this imply that they key exists in s3, when it may not? 
      IO(List(key))
    } else {
      // get all the immediate child keys
      val keyAndItsChildrenIo = IO {

        val listing = s3.listObjects(opts.config.aws.s3.bucket, key)
  
        keysFrom(listing)
      }
  
      // scan all the child keys recursively
      def recurse = keyAndItsChildrenIo.flatMap { keys =>
        //Filter ourselves out, or else we loop forever!
        val childKeysOnly = keys.filter(_ != key)
        
        val descendantsIo: IO[List[List[String]]] = childKeysOnly.map(ls(_, true, pathSep)).sequence
        
        //flatten, and add the "root" back in
        descendantsIo.map(_.flatten).map(key +: _)
      }
      
      // immediate children only
      if (recursive) { recurse } 
      else { keyAndItsChildrenIo }
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
        (new DeleteObjectsRequest(opts.config.aws.s3.bucket)).withKeys(keyVersions.asJava)
      }
      
      val responseIOs = requests.map(request => IO(s3.deleteObjects(request)))
      
      responseIOs.toList.sequence
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
      metadata <- put(s"$name/_metadata", metadata)
    } yield dir -> metadata
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
  
  private object Implicits {
    final implicit class RichObjectListing(val listing: ObjectListing) {
      def iterator: Iterator[ObjectListing] = Iterator(listing) ++ new Iterator[ObjectListing] {
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
}
