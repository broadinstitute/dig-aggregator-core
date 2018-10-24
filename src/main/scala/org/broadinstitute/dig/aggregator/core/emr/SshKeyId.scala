package org.broadinstitute.dig.aggregator.core.emr

/**
 * @author clint
 * Oct 24, 2018
 */
final case class SshKeyId(value: String)

object SshKeyId {
  val genomeStoreRest: SshKeyId = SshKeyId("GenomeStore REST")
}
