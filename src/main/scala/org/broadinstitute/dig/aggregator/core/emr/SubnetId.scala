package org.broadinstitute.dig.aggregator.core.emr

/**
 * @author clint
 * Oct 24, 2018
 */
final case class SubnetId(value: String)

object SubnetId {
  val restServices: SubnetId = SubnetId("subnet-ab89bbf3")
}
