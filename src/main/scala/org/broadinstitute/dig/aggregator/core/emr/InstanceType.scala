package org.broadinstitute.dig.aggregator.core.emr

/**
 * @author clint
 * Oct 24, 2018
 */
final case class InstanceType(value: String)

object InstanceType {
  val m3Large: InstanceType = InstanceType("m3.large")
  val m3XLarge: InstanceType = InstanceType("m3.xlarge")
}
