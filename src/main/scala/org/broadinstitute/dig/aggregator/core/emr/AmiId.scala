package org.broadinstitute.dig.aggregator.core.emr

/**
 * @author clint
 * Oct 24, 2018
 */
final case class AmiId(value: String)

object AmiId {
  val amazonLinux2018Dot03: AmiId = AmiId("ami-f316478c")
}
