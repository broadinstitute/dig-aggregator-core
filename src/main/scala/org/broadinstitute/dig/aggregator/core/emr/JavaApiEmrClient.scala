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
      clusterName: String,
      applications: Seq[ApplicationName],
      instances: Int,
      releaseLabel: EmrReleaseId,
      serviceRole: RoleId,
      jobFlowRole: RoleId,
      autoScalingRole: RoleId,
      visibleToAllUsers: Boolean,
      sshKeyName: SshKeyId,
      keepJobFlowAliveWhenNoSteps: Boolean,
      masterInstanceType: InstanceType, 
      slaveInstanceType: InstanceType,
      bootstrapScripts: Seq[URI],
      securityGroupIds: Seq[SecurityGroupId],
      subnetId: SubnetId,
      logKey: String,
      amiId: Option[AmiId]
    ): IO[EmrClusterId] = IO {
    
    val request = JavaApiEmrClient.makeClusterCreationRequest(
        clusterName,
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
        securityGroupIds, 
        subnetId, 
        aws.uriOf(logKey), 
        amiId)
        
    logger.debug("Making EMR cluster-creation request...")
        
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
  
  override def runSparkJobOnCluster(
      clusterId: EmrClusterId, 
      scriptUri: URI, 
      scriptArgs: String*): IO[Option[EmrStepId]] = runOnCluster(clusterId, JobStep.PySpark(scriptUri, scriptArgs: _*))
  
  override def runScriptOnCluster(
      clusterId: EmrClusterId, 
      scriptUri: URI, 
      scriptArgs: String*): IO[Option[EmrStepId]] = runOnCluster(clusterId, JobStep.Script(scriptUri, scriptArgs: _*))
  
  private def runOnCluster(clusterId: EmrClusterId, jobStep: JobStep): IO[Option[EmrStepId]] = IO {
    val request = (new AddJobFlowStepsRequest)
      .withJobFlowId(clusterId.value)
      .withSteps(jobStep.config)

    val result = aws.emr.addJobFlowSteps(request)

    val stepIdOpt = result.getStepIds.asScala.headOption
    
    val idMessagePart: String = stepIdOpt match {
      case Some(id) => s"assigned step id: '$id'"
      case None => "NOT assigned a step id; something failed."
    }
    
    // show the job ID so it can be referenced in the AWS console
    logger.debug(s"Submitted job with 1 steps: '${jobStep}' ${idMessagePart}")
    
    stepIdOpt.map(EmrStepId(_))
  }
}

object JavaApiEmrClient {
  private[core] def isSuccess(statusCode: Int): Boolean = statusCode == 200
  
  private[core] def makeClusterCreationRequest(
      clusterName: String,
      applications: Seq[ApplicationName],
      instances: Int,
      releaseLabel: EmrReleaseId,
      serviceRole: RoleId,
      jobFlowRole: RoleId,
      autoScalingRole: RoleId,
      visibleToAllUsers: Boolean,
      sshKeyName: SshKeyId,
      keepJobFlowAliveWhenNoSteps: Boolean,
      masterInstanceType: InstanceType, 
      slaveInstanceType: InstanceType,
      bootstrapScripts: Seq[URI],
      securityGroupIds: Seq[SecurityGroupId],
      subnetId: SubnetId,
      logUri: URI,
      amiId: Option[AmiId]): RunJobFlowRequest = {
    
    val bootstrapScriptConfigs: Seq[BootstrapActionConfig] = bootstrapScripts.map { scriptUri =>
      (new BootstrapActionConfig).withScriptBootstrapAction(
          (new ScriptBootstrapActionConfig).withPath(scriptUri.toString)).withName(scriptUri.toString)
    }
    
    def addCustomAmiIdIfSupplied(r: RunJobFlowRequest): RunJobFlowRequest = amiId match {
      case Some(AmiId(id)) => r.withCustomAmiId(id)
      case _ => r
    }
    
    addCustomAmiIdIfSupplied((new RunJobFlowRequest)
      .withName(clusterName)
      .withBootstrapActions(bootstrapScriptConfigs.asJava)
      .withApplications(applications.map(_.toApplication).asJava)
      .withReleaseLabel(releaseLabel.value)
      .withServiceRole(serviceRole.value)
      .withJobFlowRole(jobFlowRole.value)
      .withAutoScalingRole(autoScalingRole.value)
      .withLogUri(logUri.toString)
      .withVisibleToAllUsers(visibleToAllUsers)
      .withInstances((new JobFlowInstancesConfig)
        .withAdditionalMasterSecurityGroups(securityGroupIds.map(_.value): _*)
        .withAdditionalSlaveSecurityGroups(securityGroupIds.map(_.value): _*)
        .withEc2SubnetId(subnetId.value)
        .withEc2KeyName(sshKeyName.value)
        .withInstanceCount(instances)
        .withKeepJobFlowAliveWhenNoSteps(keepJobFlowAliveWhenNoSteps)
        .withMasterInstanceType(masterInstanceType.value)
        .withSlaveInstanceType(slaveInstanceType.value)))
  }
}
