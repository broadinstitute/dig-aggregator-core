package org.broadinstitute.dig.aggregator.core.emr

import java.net.URI

import scala.collection.Seq

import com.amazonaws.services.elasticmapreduce.model.ClusterSummary

import cats.effect.IO

import EmrClient.Defaults

/**
 * @author clint
 * Oct 12, 2018
 */
trait EmrClient {
  def createCluster(
      applications: Seq[ApplicationName] = Defaults.applications,
      instances: Int = Defaults.instances,
      releaseLabel: EmrReleaseId = Defaults.releaseLabel,
      serviceRole: RoleId = Defaults.serviceRole,
      jobFlowRole: RoleId = Defaults.jobFlowRole,
      autoScalingRole: RoleId = Defaults.autoScalingRole,
      visibleToAllUsers: Boolean = Defaults.visibleToAllUsers,
      sshKeyName: SshKeyId = Defaults.sshKeyName,
      keepJobFlowAliveWhenNoSteps: Boolean = Defaults.keepJobFlowAliveWhenNoSteps,
      masterInstanceType: InstanceType = Defaults.masterInstanceType, 
      slaveInstanceType: InstanceType = Defaults.slaveInstanceType,
      bootstrapScripts: Seq[URI] = Defaults.bootstrapScripts,
      securityGroupIds: Seq[SecurityGroupId] = Defaults.securityGroupIds,
      subnetId: SubnetId = Defaults.subnetId,
      logKey: String = Defaults.logBaseKey,
      amiId: Option[AmiId] = None
    ): IO[EmrClusterId]
  
  def listClusters: IO[Seq[ClusterSummary]]
  
  final def listClustersIds: IO[Seq[EmrClusterId]] = {
    listClusters.map(_.map(summary => EmrClusterId(summary.getId)))
  }
  
  def deleteCluster(clusterId: EmrClusterId): IO[Boolean]
  
  def runOnCluster(clusterId: EmrClusterId, scriptUri: URI, scriptArgs: String*): IO[Option[EmrStepId]]
}

object EmrClient {
  object Defaults {
    import ApplicationName.{ Hadoop, Spark, Hive, Pig }
    
    val applications: Seq[ApplicationName] = Seq(Hadoop, Spark, Hive, Pig)
    val instances: Int = 1
    val releaseLabel: EmrReleaseId = EmrReleaseId.emr5Dot17Dot0
    val serviceRole: RoleId = RoleId.emrDefaultRole
    val jobFlowRole: RoleId = RoleId.emrEc2DefaultRole 
    val autoScalingRole: RoleId = RoleId.emrAutoScalingDefaultRole
    val visibleToAllUsers: Boolean = true
    val sshKeyName: SshKeyId = SshKeyId.genomeStoreRest
    val keepJobFlowAliveWhenNoSteps: Boolean = true
    //note: m1.small doesn't have enough ram; m1.medium runs very slowly.
    val masterInstanceType: InstanceType = InstanceType.m3.large
    //note: m1.small doesn't have enough ram; m1.medium runs very slowly.
    val slaveInstanceType: InstanceType = InstanceType.m3.large
    val bootstrapScripts: Seq[URI] = Nil
    val securityGroupIds: Seq[SecurityGroupId] = Seq(SecurityGroupId.digAnalysisGroup)
    val subnetId: SubnetId = SubnetId.restServices
    val logBaseKey: String = "cluster-logs"
  }
}
