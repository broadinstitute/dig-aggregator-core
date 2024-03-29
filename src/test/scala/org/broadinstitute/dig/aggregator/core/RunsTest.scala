package org.broadinstitute.dig.aggregator.core

import java.time.LocalDateTime
import java.time.temporal.ChronoUnit

import org.scalatest.funsuite.AnyFunSuite

final class RunsTest extends AnyFunSuite {
  implicit val context: Context = new TestContext(TestMethod)

  // instantiate a stage to use as a test
  private val testStage = new TestMethod.TestStage()

  // create a dummy input with current time
  def input(name: String): Input = {
    // truncate to millis for test so == works
    Input(name, LocalDateTime.now.truncatedTo(ChronoUnit.MILLIS))
  }

  test("migrate") {
    Runs.migrate()
    assert(Runs.all().isEmpty)
  }

  test("insert/delete - single input") {
    Runs.migrate()

    // insert a single run
    Runs.insert(testStage, "o1", Seq(input("i1")))
    assert(Runs.all().size == 1)

    // delete run
    Runs.delete(testStage, "o1")
    assert(Runs.all().isEmpty)
  }

  test("insert/delete - multiple inputs/outputs") {
    val inputs = (1 to 6).map(_.toString).map(input)

    Runs.migrate()

    // insert multiple inputs for a single output
    Runs.insert(testStage, "o1", inputs.take(3))
    Runs.insert(testStage, "o2", inputs.drop(3))

    // ensure the inputs match outputs
    val results = Runs.of(testStage)

    // db should only have 6 rows
    assert(results.size == 6)

    // get the runs of output 1 and 2
    val o1 = results.filter(_.output == "o1")
    val o2 = results.filter(_.output == "o2")

    // get the inputs of o1 and o2
    val i1 = o1.map(r => r.input -> r.version).map((Input.apply _).tupled).toSet
    val i2 = o2.map(r => r.input -> r.version).map((Input.apply _).tupled).toSet

    // ensure the inputs match
    assert(i1 == inputs.take(3).toSet)
    assert(i2 == inputs.drop(3).toSet)

    // delete runs
    Runs.delete(testStage, "o1")
    Runs.delete(testStage, "o2")
    assert(Runs.all().isEmpty)
  }

  test("update output with changed inputs") {
    val inputs = (1 to 3).map(_.toString).map(input)

    Runs.migrate()

    // insert a single output with 3 outputs
    Runs.insert(testStage, "o", inputs)

    // get all the inputs and verify them
    val i1 = Runs.all().map(r => r.input -> r.version).map((Input.apply _).tupled).toSet
    assert(i1 == inputs.toSet)

    // insert the same output with 3 new outputs
    val newInputs = (4 to 6).map(_.toString).map(input)
    Runs.insert(testStage, "o", newInputs)

    // get the new inputs and verify them
    val i2 = Runs.all().map(r => r.input -> r.version).map((Input.apply _).tupled).toSet
    assert(i2 == (inputs ++ newInputs).toSet)

    // insert the updated inputs (same key, different version)
    val updatedInputs = inputs.map(i => input(i.key))
    Runs.insert(testStage, "o", updatedInputs)

    // get the new inputs and verify them
    val i3 = Runs.all().map(r => r.input -> r.version).map((Input.apply _).tupled).toSet
    assert(i3 == (newInputs ++ updatedInputs).toSet)

    // clean up
    Runs.delete(testStage, "o")
    assert(Runs.all().isEmpty)
  }
}
