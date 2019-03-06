package org.broadinstitute.dig.aggregator.core.emr

import com.amazonaws.services.elasticmapreduce.model.AddJobFlowStepsRequest
import com.amazonaws.services.elasticmapreduce.model.AddJobFlowStepsResult
import com.amazonaws.services.elasticmapreduce.model.Application
import com.amazonaws.services.elasticmapreduce.model.BootstrapActionConfig
import com.amazonaws.services.elasticmapreduce.model.Configuration
import com.amazonaws.services.elasticmapreduce.model.EbsBlockDeviceConfig
import com.amazonaws.services.elasticmapreduce.model.EbsConfiguration
import com.amazonaws.services.elasticmapreduce.model.InstanceGroupConfig
import com.amazonaws.services.elasticmapreduce.model.InstanceRoleType
import com.amazonaws.services.elasticmapreduce.model.JobFlowDetail
import com.amazonaws.services.elasticmapreduce.model.JobFlowInstancesConfig
import com.amazonaws.services.elasticmapreduce.model.ListStepsRequest
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowRequest
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowResult
import com.amazonaws.services.elasticmapreduce.model.ScriptBootstrapActionConfig
import com.amazonaws.services.elasticmapreduce.model.StepState
import com.amazonaws.services.elasticmapreduce.model.StepSummary
import com.amazonaws.services.elasticmapreduce.model.VolumeSpecification

import java.net.URI

import scala.collection.JavaConverters._

/**
 * Parameterized configuration for an EMR cluster. Constant settings are
 * located in `config.emr.EmrConfig` and are loaded in the JSON.
 */
final case class Cluster(
    name: String,
    amiId: Option[AmiId] = None, //AmiId.amazonLinux_2018_3,
    instances: Int = 3,
    masterInstanceType: InstanceType = InstanceType.m5_4xlarge,
    slaveInstanceType: InstanceType = InstanceType.m5_2xlarge,
    masterVolumeSizeInGB: Int = 32,
    slaveVolumeSizeInGB: Int = 32,
    applications: Seq[ApplicationName] = Cluster.defaultApplications,
    configurations: Seq[ApplicationConfig] = Cluster.defaultConfigurations,
    bootstrapScripts: Seq[BootstrapScript] = Seq(),
    keepAliveWhenNoSteps: Boolean = false,
    visibleToAllUsers: Boolean = true
) {
  require(name.matches("[A-Za-z_]+[A-Za-z0-9_]*"), s"Illegal cluster name: $name")
  require(instances >= 1)

  /**
   */
  val masterInstanceGroupConfig: InstanceGroupConfig = {
    val volumeSpec = new VolumeSpecification()
      .withSizeInGB(masterVolumeSizeInGB)
      .withVolumeType("gp2")

    val deviceConfig = new EbsBlockDeviceConfig().withVolumeSpecification(volumeSpec)
    val ebsConfig    = new EbsConfiguration().withEbsBlockDeviceConfigs(deviceConfig)

    new InstanceGroupConfig()
      .withEbsConfiguration(ebsConfig)
      .withInstanceType(masterInstanceType.value)
      .withInstanceRole(InstanceRoleType.MASTER)
      .withInstanceCount(1)
  }

  /**
   */
  val slaveInstanceGroupConfig: InstanceGroupConfig = {
    val volumeSpec = new VolumeSpecification()
      .withSizeInGB(slaveVolumeSizeInGB)
      .withVolumeType("gp2")

    val deviceConfig = new EbsBlockDeviceConfig().withVolumeSpecification(volumeSpec)
    val ebsConfig    = new EbsConfiguration().withEbsBlockDeviceConfigs(deviceConfig)

    new InstanceGroupConfig()
      .withEbsConfiguration(ebsConfig)
      .withInstanceType(slaveInstanceType.value)
      .withInstanceRole(InstanceRoleType.TASK)
      .withInstanceCount(instances - 1)
  }

  /**
   */
  val instanceGroups: Seq[InstanceGroupConfig] = {
    Seq(masterInstanceGroupConfig, slaveInstanceGroupConfig)
      .filter(_.getInstanceCount > 0)
  }
}

/**
 * Companion object for creating an EMR cluster.
 */
object Cluster {

  /** The default set used by pretty much every cluster. */
  val defaultApplications: Seq[ApplicationName] = Seq(
    ApplicationName.hadoop,
    ApplicationName.spark,
    ApplicationName.hive,
    ApplicationName.pig,
    ApplicationName.hue
  )

  /** The default configurations for applications. */
  val defaultConfigurations: Seq[ApplicationConfig] = Seq()
}
