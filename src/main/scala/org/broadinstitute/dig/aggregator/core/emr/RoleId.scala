package org.broadinstitute.dig.aggregator.core.emr

/**
 * @author clint
 * Oct 24, 2018
 */
final case class RoleId(value: String)

object RoleId {
  val emrDefaultRole: RoleId = RoleId("EMR_DefaultRole")
  val emrEc2DefaultRole: RoleId = RoleId("EMR_EC2_DefaultRole")
  val emrAutoScalingDefaultRole: RoleId = RoleId("EMR_AutoScaling_DefaultRole")
}
