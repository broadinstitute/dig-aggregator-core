package org.broadinstitute.dig.aggregator.core.emr

import org.scalatest.FunSuite

/**
 * @author clint
 * Oct 30, 2018
 */
final class EmrReleaseIdTest extends FunSuite {
  test("constants") {
    assert(EmrReleaseId.emr5Dot17Dot0.value === "emr-5.17.0")
  }
}
