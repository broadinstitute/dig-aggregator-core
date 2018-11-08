package org.broadinstitute.dig.aggregator.core.emr

import com.amazonaws.services.elasticmapreduce

import java.net.URI

/**
 * Parameterized configuration for an EMR cluster. Constant settings are
 * located in `config.emr.EmrConfig` and are loaded in the JSON.
 */
final case class Cluster(
    name: String,
    ami: Option[AmiId] = None, //AmiId.amazonLinux_2018_3,
    instances: Int = 3,
    masterInstanceType: InstanceType = InstanceType.m5_4xlarge,
    slaveInstanceType: InstanceType = InstanceType.m5_2xlarge,
    applications: Seq[Cluster.App] = Cluster.defaultApplications,
    bootstrapScripts: Seq[URI] = Seq(),
    keepAliveWhenNoSteps: Boolean = false,
    visibleToAllUsers: Boolean = true,
)

/**
 * Companion object for creating an EMR cluster.
 */
object Cluster {

  /** An application that can be pre-installed on the cluster. */
  sealed case class App(value: String) {
    def application = new elasticmapreduce.model.Application().withName(value)
  }

  /** Applications understood by AWS to be installed with the cluster. */
  val hadoop: App = App("Hadoop")
  val spark: App  = App("Spark")
  val hive: App   = App("Hive")
  val pig: App    = App("Pig")

  /** The default set used by pretty much every cluster. */
  val defaultApplications = Seq(hadoop, spark, hive, pig)
}
