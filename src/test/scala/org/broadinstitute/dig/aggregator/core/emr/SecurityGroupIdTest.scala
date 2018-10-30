package org.broadinstitute.dig.aggregator.core.emr

import org.scalatest.FunSuite

/**
 * @author clint
 * Oct 30, 2018
 */
final class SecurityGroupIdTest extends FunSuite {
  test("constants") {
    assert(SecurityGroupId.digAnalysisGroup.value === "sg-2b58c961")
  }
}
