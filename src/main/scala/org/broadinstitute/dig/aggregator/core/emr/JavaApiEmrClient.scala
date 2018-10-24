package org.broadinstitute.dig.aggregator.core.emr

import java.net.URI

import scala.collection.JavaConverters._

import org.broadinstitute.dig.aggregator.core.AWS
import org.broadinstitute.dig.aggregator.core.JobStep

import com.amazonaws.services.elasticmapreduce.model.AddJobFlowStepsRequest
import com.amazonaws.services.elasticmapreduce.model.Application
import com.amazonaws.services.elasticmapreduce.model.BootstrapActionConfig
import com.amazonaws.services.elasticmapreduce.model.ClusterSummary
import com.amazonaws.services.elasticmapreduce.model.JobFlowInstancesConfig
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowRequest
import com.amazonaws.services.elasticmapreduce.model.ScriptBootstrapActionConfig
import com.amazonaws.services.elasticmapreduce.model.TerminateJobFlowsRequest
import com.typesafe.scalalogging.LazyLogging

import EmrClient.Defaults
import cats.effect.IO

/**
 * @author clint
 * Oct 12, 2018
 */
final class JavaApiEmrClient(aws: AWS) extends EmrClient with LazyLogging {
  override def createCluster(
      applications: Seq[ApplicationName] = Defaults.applications,
      instances: Int = Defaults.instances,
      releaseLabel: String = Defaults.releaseLabel,
      serviceRole: String = Defaults.serviceRole,
      jobFlowRole: String = Defaults.jobFlowRole,
      autoScalingRole: String = Defaults.autoScalingRole,
      visibleToAllUsers: Boolean = Defaults.visibleToAllUsers,
      sshKeyName: String = Defaults.sshKeyName,
      keepJobFlowAliveWhenNoSteps: Boolean = Defaults.keepJobFlowAliveWhenNoSteps,
      masterInstanceType: String = Defaults.masterInstanceType, 
      slaveInstanceType: String = Defaults.slaveInstanceType,
      bootstrapScripts: Seq[URI] = Defaults.bootstrapScripts,
      securityGroupId: String = Defaults.securityGroupId,
      subnetId: String = Defaults.subnetId,
      logKey: String = Defaults.logBaseKey,
      amiId: Option[AmiId] = None
    ): IO[EmrClusterId] = IO {
    
    val request = JavaApiEmrClient.makeRequest(
        applications, 
        instances, 
        releaseLabel, 
        serviceRole, 
        jobFlowRole, 
        autoScalingRole, 
        visibleToAllUsers, 
        sshKeyName, 
        keepJobFlowAliveWhenNoSteps, 
        masterInstanceType, 
        slaveInstanceType, 
        bootstrapScripts, 
        securityGroupId, 
        subnetId, 
        aws.uriOf(logKey), 
        amiId)
        
    println("Making request...")
        
    val result = aws.emr.runJobFlow(request)
    
    EmrClusterId(result.getJobFlowId)
  }
  
  override def listClusters: IO[Seq[ClusterSummary]] = IO { 
    aws.emr.listClusters.getClusters.asScala
  }
  
  override def deleteCluster(clusterId: EmrClusterId): IO[Boolean] = IO {
    val request = (new TerminateJobFlowsRequest).withJobFlowIds(clusterId.value)
    
    val result = aws.emr.terminateJobFlows(request)
    
    val statusCode = result.getSdkHttpMetadata.getHttpStatusCode
    def responseHeaders = result.getSdkHttpMetadata.getHttpHeaders.asScala.toMap
    
    logger.debug(s"Deleted '$clusterId': status: $statusCode response headers: $responseHeaders")
    
    JavaApiEmrClient.isSuccess(statusCode)
  }
  
  override def runOnCluster(clusterId: EmrClusterId, scriptUri: URI, scriptArgs: String*): IO[Option[EmrStepId]] = IO {
    val request = (new AddJobFlowStepsRequest)
      .withJobFlowId(clusterId.value)
      .withSteps(JobStep.PySpark(scriptUri, scriptArgs: _*).config)

    val result = aws.emr.addJobFlowSteps(request)

    val stepIdOpt = result.getStepIds.asScala.headOption
    
    val idMessagePart: String = stepIdOpt match {
      case Some(id) => s"assigned step id: '$id'"
      case None => "NOT assigned a step id; something failed."
    }
    
    // show the job ID so it can be referenced in the AWS console
    logger.debug(s"Submitted job with 1 steps: '${scriptUri}' ${idMessagePart}")
    
    stepIdOpt.map(EmrStepId(_))
  }
}

object JavaApiEmrClient {
  private[core] def isSuccess(statusCode: Int): Boolean = statusCode == 200
  
  private[core] def makeRequest(
      applications: Seq[String],
      instances: Int,
      releaseLabel: String,
      serviceRole: String,
      jobFlowRole: String,
      autoScalingRole: String,
      visibleToAllUsers: Boolean,
      sshKeyName: String,
      keepJobFlowAliveWhenNoSteps: Boolean,
      masterInstanceType: String, 
      slaveInstanceType: String,
      bootstrapScripts: Seq[URI],
      securityGroupId: String,
      subnetId: String,
      logUri: URI,
      amiId: Option[AmiId]): RunJobFlowRequest = {
    
    val bootstrapScriptConfigs: Seq[BootstrapActionConfig] = bootstrapScripts.map { scriptUri =>
      (new BootstrapActionConfig).withScriptBootstrapAction(
          (new ScriptBootstrapActionConfig).withPath(scriptUri.toString)).withName(scriptUri.toString)
    }
    
    def toApplication(name: String): Application = (new Application).withName(name)
    
    def addCustomAmiIdIfSupplied(r: RunJobFlowRequest): RunJobFlowRequest = amiId match {
      case Some(AmiId(id)) => r.withCustomAmiId(id)
      case _ => r
    }
    
    addCustomAmiIdIfSupplied((new RunJobFlowRequest)
      .withName("Clint's Spark Cluster")
      .withBootstrapActions(bootstrapScriptConfigs.asJava)
      .withApplications(applications.map(toApplication).asJava)
      .withReleaseLabel(releaseLabel)
      .withServiceRole(serviceRole)
      .withJobFlowRole(jobFlowRole)
      .withAutoScalingRole(autoScalingRole)
      .withLogUri(logUri.toString)
      .withVisibleToAllUsers(visibleToAllUsers)
      .withInstances((new JobFlowInstancesConfig)
        .withAdditionalMasterSecurityGroups(securityGroupId)
        .withAdditionalSlaveSecurityGroups(securityGroupId)
        .withEc2SubnetId(subnetId)
        .withEc2KeyName(sshKeyName)
        .withInstanceCount(instances)
        .withKeepJobFlowAliveWhenNoSteps(keepJobFlowAliveWhenNoSteps)
        .withMasterInstanceType(masterInstanceType)
        .withSlaveInstanceType(slaveInstanceType)))
  }
}
