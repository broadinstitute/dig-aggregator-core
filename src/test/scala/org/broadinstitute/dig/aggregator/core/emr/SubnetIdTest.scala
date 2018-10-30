package org.broadinstitute.dig.aggregator.core.emr

import org.scalatest.FunSuite

/**
 * @author clint
 * Oct 30, 2018
 */
final class SubnetIdTest extends FunSuite {
  test("constants") {
    assert(SubnetId.restServices.value === "subnet-ab89bbf3")
  }
}
