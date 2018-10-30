package org.broadinstitute.dig.aggregator.core.emr

import org.scalatest.FunSuite

/**
 * @author clint
 * Oct 30, 2018
 */
final class AmiIdTest extends FunSuite {
  test("constants") {
    assert(AmiId.amazonLinux2018Dot03.value === "ami-f316478c")
    
    assert(AmiId.customAmiWithMetalAndLdsc.value === "ami-05d585056c5a2c2b7")
  }
}
