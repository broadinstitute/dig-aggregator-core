package org.broadinstitute.dig.aggregator.core.emr

/**
 * When creating an EMR cluster, you provide the AMI (EC2 image instance)
 * to clone for the master and slave nodes.
 */
final case class AmiId(value: String) {
  require(value.startsWith("ami-"), s"Invalid AMI ID: '$value'")
}

/**
 * Companion object with global AMIs.
 */
object AmiId {

  /** Constant AMI provided by AWS with nothing special. */
  val amazonLinux_2018_3: AmiId = AmiId("ami-f316478c")
}
