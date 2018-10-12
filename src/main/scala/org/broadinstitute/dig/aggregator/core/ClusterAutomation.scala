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
    
    aws.put("hello-spark.py", helloSparkContents).unsafeRunSync()
    
    val bootstrapScriptContents: String = """
      #!/bin/bash -xe
      
      sudo yum install -y amazon-efs-utils
      
      sudo mkdir -p /mnt/efs
      sudo mount -t efs fs-06254a4d:/ /mnt/efs

      sudo pip install boto==2.39.0
      sudo pip install neo4j-driver==1.6.1
      sudo pip install scipy==1.1.0
      """
    
    aws.put("cluster-bootstrap.sh", bootstrapScriptContents).unsafeRunSync()
    
    val id = makeCluster(
        bootstrapScripts = Seq(aws.uriOf("cluster-bootstrap.sh")),
        masterInstanceType = "m3.xlarge",
        slaveInstanceType = "m3.xlarge")
        

    println(s"Made request, job flow id = '${id}'")
    
    runOnCluster(id, aws.uriOf("hello-spark.py"))
    
    val clusters = listClusters
    
    println(s"${clusters.size} clusters:")
    
    clusters.foreach(println)
  }
  
  def runOnCluster(clusterId: String, scriptUri: URI, scriptArgs: String*): Unit = {
    val request = (new AddJobFlowStepsRequest)
      .withJobFlowId(clusterId)
      .withSteps(JobStep.PySpark(scriptUri, scriptArgs: _*).config)

    val job = aws.emr.addJobFlowSteps(request)

    // show the job ID so it can be referenced in the AWS console
    println(s"Submitted job with 1 steps: ${scriptUri}")
  }
  
  def listClusters: Seq[ClusterSummary] = aws.emr.listClusters.getClusters.asScala
  
  def deleteCluster(clusterId: String): Unit = {
    val request = (new TerminateJobFlowsRequest)
    
    request.withJobFlowIds(clusterId)
    
    val result = aws.emr.terminateJobFlows(request)
    
    val statusCode = result.getSdkHttpMetadata.getHttpStatusCode
    val responseHeaders = result.getSdkHttpMetadata.getHttpHeaders.asScala.toMap
    
    println(s"Deleted '$clusterId': status: $statusCode response headers: $responseHeaders")
  }
  
  def makeCluster(
      sparkApp: Application = new Application,
      appName: String = "Spark",
      instances: Int = 1,
      releaseLabel: String = "emr-5.17.0",
      serviceRole: String = "EMR_DefaultRole",
      jobFlowRole: String = "EMR_EC2_DefaultRole",
      autoScalingRole: String = "EMR_AutoScaling_DefaultRole",
      visibleToAllUsers: Boolean = true,
      sshKeyName: String = "GenomeStore REST",
      keepJobFlowAliveWhenNoSteps: Boolean = true,
      masterInstanceType: String = "m1.large", //note: m1.small doesn't have enough ram!
      slaveInstanceType: String = "m1.large",
      bootstrapScripts: Seq[URI] = Nil,
      logUri: String = aws.uriOf("cluster-logs").toString
    ): String = {
    
    val bootstrapScriptConfigs: Seq[BootstrapActionConfig] = bootstrapScripts.map { scriptUri =>
      (new BootstrapActionConfig).withScriptBootstrapAction(
          (new ScriptBootstrapActionConfig).withPath(scriptUri.toString)).withName(scriptUri.toString)
    }
    
    val securityGroupId = "sg-2b58c961"
    
    val masterNodeSg = "sg-a976afe2"
    val slaveNodeSg = "sg-bc7fa6f7"
    
    val subnetId = "subnet-ab89bbf3"
    val vpcId = "vpc-a53ba7c2"
    
    val request = (new RunJobFlowRequest)
      .withName("Clint's Spark Cluster")
      .withBootstrapActions(bootstrapScriptConfigs.asJava)
      .withApplications(sparkApp.withName(appName))
      .withReleaseLabel(releaseLabel)
      .withServiceRole(serviceRole)
      .withJobFlowRole(jobFlowRole)
      .withAutoScalingRole(autoScalingRole)
      .withLogUri(logUri)
      .withVisibleToAllUsers(visibleToAllUsers)
      .withInstances((new JobFlowInstancesConfig)
        //.withServiceAccessSecurityGroup(securityGroupId)
        .withAdditionalMasterSecurityGroups(securityGroupId)
        .withAdditionalSlaveSecurityGroups(securityGroupId)
        .withEc2SubnetId(subnetId)
        .withEc2KeyName(sshKeyName)
        .withInstanceCount(instances)
        .withKeepJobFlowAliveWhenNoSteps(keepJobFlowAliveWhenNoSteps)
        .withMasterInstanceType(masterInstanceType)
        .withSlaveInstanceType(slaveInstanceType))
        
    println("Making request...")
        
    val result = aws.emr.runJobFlow(request)
    
    result.getJobFlowId
  }
}
