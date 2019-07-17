package org.broadinstitute.dig.aggregator.core.config.emr

import org.json4s._

/** AWS EMR settings for creating a job cluster. These remain constant across
  * all clusters created: subnet, SSH keys, EMR release, roles, etc.
  */
final case class EmrConfig(
    sshKeyName: String,
    subnetId: SubnetId,
    releaseLabel: ReleaseLabel = ReleaseLabel.emrLatest,
    securityGroupIds: Seq[SecurityGroupId] = Seq(),
    serviceRoleId: RoleId = RoleId.defaultRole,
    jobFlowRoleId: RoleId = RoleId.ec2DefaultRole,
    autoScalingRoleId: RoleId = RoleId.autoScalingDefaultRole,
)

/** Companion object with custom JSON serializers.
  */
object EmrConfig {

  /** Custom JSON serializers for various EMR case class settings. To use
    * these when deserializing, add them like so:
    *
    * implicit val formats = json4s.DefaultFormats ++ EmrConfig.customSerializers
    */
  val customSerializers: Seq[CustomSerializer[_]] = Seq(
    ReleaseLabel.Serializer,
    RoleId.Serializer,
    SecurityGroupId.Serializer,
    SubnetId.Serializer,
  )
}
