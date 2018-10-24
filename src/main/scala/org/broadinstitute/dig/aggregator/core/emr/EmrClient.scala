package org.broadinstitute.dig.aggregator.core.emr

import java.net.URI

import scala.collection.Seq

import com.amazonaws.services.elasticmapreduce.model.ClusterSummary

import cats.effect.IO

/**
 * @author clint
 * Oct 12, 2018
 */
trait EmrClient {
  def createCluster(
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
    //note: m1.small doesn't have enough ram! m1.medium runs very slowly.
    val masterInstanceType: InstanceType = InstanceType.m3Large
    //note: m1.small doesn't have enough ram! m1.medium runs very slowly.
    val slaveInstanceType: InstanceType = InstanceType.m3Large
    val bootstrapScripts: Seq[URI] = Nil
    val securityGroupId: SecurityGroupId = SecurityGroupId.digAnalysisGroup
    val subnetId: SubnetId = SubnetId.restServices
    val logBaseKey: String = "cluster-logs"
  }
}
