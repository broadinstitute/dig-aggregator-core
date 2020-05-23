package org.broadinstitute.dig.aggregator.core

import java.util.UUID

import org.scalatest.funsuite.AnyFunSuite

final class StageTest extends AnyFunSuite with ProvidesH2Transactor {

  // create a dummy input
  def input(name: String): Input = Input(name, UUID.randomUUID.toString)

  // some fake inputs
  private val inputA1 = input("a/foo/bar") // -> Outputs.Named("foo")
  private val inputA2 = input("a/foo/baz") // -> Outputs.Named("foo")
  private val inputB1 = input("b/bar/wow") // -> Outputs.Named("bar")
  private val inputB2 = input("b/bar/ack") // -> Outputs.Named("bar")
  private val inputC1 = input("c/any/all") // -> Outputs.All

  // dummy options
  private val opts = new Opts(Seq.empty)

  def testInputToOutput(input: Input, expectedOutput: String): Boolean = {
    TestMethod.TestStage.getOutputs(input) match {
      case Outputs.Named(seq @ _*) => seq == Seq(expectedOutput)
      case _                       => false
    }
  }

  test("input -> outputs") {
    assert(testInputToOutput(inputA1, "foo"))
    assert(testInputToOutput(inputA2, "foo"))
    assert(testInputToOutput(inputB1, "bar"))
    assert(testInputToOutput(inputB2, "bar"))
  }

  test("all outputs") {
    assert(TestMethod.TestStage.getOutputs(inputC1) == Outputs.All)
  }

  test("simple (output -> inputs)") {
    val inputs    = Seq(inputA1, inputA2, inputB1, inputB2)
    val outputMap = TestMethod.TestStage.buildOutputMap(inputs, opts)

    // should only contain the outputs foo and bar
    assert(outputMap.keys.size == 2)
    assert(outputMap.contains("foo"))
    assert(outputMap.contains("bar"))

    // ensure all the inputs map to the correct outputs
    assert(outputMap("foo") == Set(inputA1, inputA2))
    assert(outputMap("bar") == Set(inputB1, inputB2))
  }

  test("all (output -> inputs)") {
    val inputs    = Seq(inputA1, inputB1, inputC1)
    val outputMap = TestMethod.TestStage.buildOutputMap(inputs, opts)

    // should only contain the outputs foo and bar
    assert(outputMap.keys.size == 2)
    assert(outputMap.contains("foo"))
    assert(outputMap.contains("bar"))

    // ensure the inputs mapping to all outputs are present in all outputs
    assert(outputMap("foo").contains(inputC1))
    assert(outputMap("bar").contains(inputC1))
  }
}
