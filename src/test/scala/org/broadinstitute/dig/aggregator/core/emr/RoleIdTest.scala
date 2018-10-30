package org.broadinstitute.dig.aggregator.core.emr

import org.scalatest.FunSuite

/**
 * @author clint
 * Oct 30, 2018
 */
final class RoleIdTest extends FunSuite {
  test("constants") {
    assert(RoleId.emrDefaultRole.value === "EMR_DefaultRole")
    assert(RoleId.emrEc2DefaultRole.value === "EMR_EC2_DefaultRole")
    assert(RoleId.emrAutoScalingDefaultRole.value === "EMR_AutoScaling_DefaultRole")
  }
}
