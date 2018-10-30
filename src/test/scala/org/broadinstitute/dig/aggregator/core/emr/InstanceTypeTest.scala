package org.broadinstitute.dig.aggregator.core.emr

import org.scalatest.FunSuite

final class InstanceTypeTest extends FunSuite {
  test("constants") {
    assert(InstanceType.m3.large.value === "m3.large")
    assert(InstanceType.m3.xlarge.value === "m3.xlarge")
    assert(InstanceType.m4.xlarge.value === "m4.xlarge")
  }
}
