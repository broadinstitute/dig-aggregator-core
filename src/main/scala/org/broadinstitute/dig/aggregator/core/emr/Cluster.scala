package org.broadinstitute.dig.aggregator.core.emr

import com.amazonaws.services.elasticmapreduce.model.Application
import com.amazonaws.services.elasticmapreduce.model.Configuration

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
    applications: Seq[ApplicationName] = Cluster.defaultApplications,
    configurations: Seq[ApplicationConfig] = Cluster.defaultConfigurations,
    bootstrapScripts: Seq[URI] = Seq(),
    keepAliveWhenNoSteps: Boolean = false,
    visibleToAllUsers: Boolean = true,
)

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
    ApplicationName.hue,
  )

  /** The default configurations for applications. */
  val defaultConfigurations: Seq[ApplicationConfig] = Seq()
}
