package org.broadinstitute.dig.aggregator.core.emr

/**
 * @author clint
 * Oct 24, 2018
 */
final case class InstanceType(value: String)

object InstanceType {
  object m3 {
    val large: InstanceType = InstanceType("m3.large")
    val xlarge: InstanceType = InstanceType("m3.xlarge")
  }
  
  object m4 {
    val xlarge: InstanceType = InstanceType("m4.xlarge")
  }
}
