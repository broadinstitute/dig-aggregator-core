package org.broadinstitute.dig.aggregator.core.emr

/**
 * @author clint
 * Oct 24, 2018
 */
final case class EmrReleaseId(value: String)

object EmrReleaseId {
  val emr5Dot17Dot0: EmrReleaseId = EmrReleaseId("emr-5.17.0")
}
