package org.broadinstitute.dig.aggregator.core

import scala.collection.JavaConverters._

import cats.effect.IO 
import com.amazonaws.services.elasticmapreduce.model.ClusterSummary
import java.net.URI
import EmrClient.Defaults
import com.amazonaws.services.elasticmapreduce.model.AddJobFlowStepsRequest

/**
 * @author clint
 * Oct 12, 2018
 */
abstract class EmrClient(aws: AWS) {
  def createCluster(
      applications: Seq[String] = Defaults.applications,
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
      logUri: String = aws.uriOf(Defaults.logBaseKey).toString
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
    val applications: Seq[String] = Seq("Hadoop", "Spark", "Hive", "Pig")
    val instances: Int = 1
    val releaseLabel: String = "emr-5.17.0"
    val serviceRole: String = "EMR_DefaultRole"
    val jobFlowRole: String = "EMR_EC2_DefaultRole"
    val autoScalingRole: String = "EMR_AutoScaling_DefaultRole"
    val visibleToAllUsers: Boolean = true
    val sshKeyName: String = "GenomeStore REST"
    val keepJobFlowAliveWhenNoSteps: Boolean = true
    //note: m1.small doesn't have enough ram! m1.medium runs very slowly.
    val masterInstanceType: String = "m1.large"
    //note: m1.small doesn't have enough ram! m1.medium runs very slowly.
    val slaveInstanceType: String = "m1.large"
    val bootstrapScripts: Seq[URI] = Nil
    val securityGroupId = "sg-2b58c961"
    val subnetId = "subnet-ab89bbf3"
    val logBaseKey: String = "cluster-logs"
  }
}
