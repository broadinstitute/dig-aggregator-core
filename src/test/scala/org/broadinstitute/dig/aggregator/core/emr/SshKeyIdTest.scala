package org.broadinstitute.dig.aggregator.core.emr

import org.scalatest.FunSuite

/**
 * @author clint
 * Oct 30, 2018
 */
final class SshKeyIdTest extends FunSuite {
  test("constants") {
    assert(SshKeyId.genomeStoreRest.value === "GenomeStore REST")
  }
}
