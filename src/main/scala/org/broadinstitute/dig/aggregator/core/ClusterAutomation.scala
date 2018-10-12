package org.broadinstitute.dig.aggregator.core

import scala.collection.JavaConverters._

import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient
import com.amazonaws.services.elasticmapreduce.model.Application
import com.amazonaws.services.elasticmapreduce.model.ClusterSummary
import com.amazonaws.services.elasticmapreduce.model.JobFlowInstancesConfig
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowRequest
import com.amazonaws.services.elasticmapreduce.model.TerminateJobFlowsRequest
import org.broadinstitute.dig.aggregator.app.Opts
import com.amazonaws.services.elasticmapreduce.model.BootstrapActionConfig
import com.amazonaws.services.elasticmapreduce.model.ScriptBootstrapActionConfig
import com.amazonaws.services.elasticmapreduce.model.StepConfig
import java.util.UUID
import com.amazonaws.services.elasticmapreduce.model.AddJobFlowStepsRequest
import java.net.URI
import com.amazonaws.services.elasticmapreduce.model.PlacementType

/**
 * @author clint
 * Oct 4, 2018
 */
object ClusterAutomation extends App {
  
  private val opts: Opts = new Opts(Array("--config", "src/it/resources/config.json"))
    
  private val credentials: AWSStaticCredentialsProvider = {
    new AWSStaticCredentialsProvider(new BasicAWSCredentials(opts.config.aws.key, opts.config.aws.secret))
  }
    
  private val aws: AWS = new AWS(opts.config.aws)
  
  run()
  
  def run(): Unit = {
    val helloSparkContents: String = """
      |from pyspark import SparkContext
      |from operator import add
      |
      |sc = SparkContext()
      |data = sc.parallelize(list("Hello World"))
      |counts = data.map(lambda x: (x, 1)).reduceByKey(add).sortBy(lambda x: x[1], ascending=False).collect()
      |for (word, count) in counts:
      |    print("{}: {}".format(word, count))
      |sc.stop()
      |""".stripMargin.trim
    
    val bootstrapScriptContents: String = """
      #!/bin/bash -xe
      
      sudo yum install -y amazon-efs-utils
      
      sudo mkdir -p /mnt/efs
      sudo mount -t efs fs-06254a4d:/ /mnt/efs

      sudo pip install boto==2.39.0
      sudo pip install neo4j-driver==1.6.1
      sudo pip install scipy==1.1.0
      """
    
    val client: EmrClient = new JavaApiEmrClient(aws)
    
    val io = for {
      _ <- aws.put("hello-spark.py", helloSparkContents)
      _ <- aws.put("cluster-bootstrap.sh", bootstrapScriptContents)
      id <- client.createCluster(
        bootstrapScripts = Seq(aws.uriOf("cluster-bootstrap.sh")),
        masterInstanceType = "m3.xlarge",
        slaveInstanceType = "m3.xlarge")
      _ = println(s"Made request, job flow id = '${id}'")
      _ <- client.runOnCluster(id, aws.uriOf("hello-spark.py"))
      clusters <- client.listClusters
      _ = println(s"${clusters.size} clusters:")
      _ = clusters.foreach(println)
    } yield {
      ()
    }
    
    io.unsafeRunSync()
  }
}
