package org.broadinstitute.dig.aggregator.core.emr

/**
 * @author clint
 * Oct 24, 2018
 */
final case class SecurityGroupId(value: String)

object SecurityGroupId {
  val digAnalysisGroup: SecurityGroupId = SecurityGroupId("sg-2b58c961")
}
