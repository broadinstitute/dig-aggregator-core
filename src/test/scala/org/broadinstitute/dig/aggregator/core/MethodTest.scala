package org.broadinstitute.dig.aggregator.core

import org.scalatest.funsuite.AnyFunSuite

final class MethodTest extends AnyFunSuite with ProvidesH2Transactor {

  test("stage lookup") {
    TestMethod.getStage("TestStage")
    ()
  }

  test("non-exitant stage") {
    assertThrows[NoSuchElementException] {
      TestMethod.getStage("doesn't exist")
    }
  }
}
