package org.broadinstitute.dig.aggregator.core

import java.time.LocalDateTime
import java.time.temporal.ChronoUnit

import org.scalatest.funsuite.AnyFunSuite

final class RunsStatusTest extends AnyFunSuite {
  implicit val context: Context = new TestContext(TestMethod)

  // instantiate a stage to use as a test
  private val testStage = new TestMethod.TestStage()

  test("migrate") {
    RunStatus.migrate()
    assert(RunStatus.all().isEmpty)
  }

  test("insert/delete - single input") {
    RunStatus.migrate()

    // insert a single run
    RunStatus.insert(testStage, "o1")
    assert(RunStatus.all().size == 1)

    // delete run
    RunStatus.delete(testStage, "o1")
    assert(RunStatus.all().isEmpty)
  }

  test("insert/delete") {

    RunStatus.migrate()

    RunStatus.insert(testStage, "o1")
    RunStatus.insert(testStage, "o2")

    // ensure the inputs match outputs
    val results = RunStatus.of(testStage)

    // db should only have 6 rows
    assert(results.size == 2)

    // get the runs of output 1 and 2
    val o1 = results.filter(_.output == "o1")
    val o2 = results.filter(_.output == "o2")
    assert(o1.size == 1)
    assert(o2.size == 1)

    // delete runs
    RunStatus.delete(testStage, "o1")
    RunStatus.delete(testStage, "o2")
    assert(RunStatus.all().isEmpty)
  }

  test("update output, removing start / end") {
    RunStatus.migrate()

    // insert a single output with 3 outputs
    RunStatus.insert(testStage, "o")
    val initialOutput = RunStatus.of(testStage).filter(_.output == "o")
    assert(initialOutput.length == 1)
    assert(initialOutput.head.started.isEmpty)
    assert(initialOutput.head.ended.isEmpty)
    RunStatus.start(testStage, "o")
    val startedOutput = RunStatus.of(testStage).filter(_.output == "o")
    assert(startedOutput.length == 1)
    assert(startedOutput.head.started.isDefined)
    assert(startedOutput.head.ended.isEmpty)
    RunStatus.end(testStage, "o")
    val endedOutput = RunStatus.of(testStage).filter(_.output == "o")
    assert(endedOutput.length == 1)
    assert(endedOutput.head.started.isDefined)
    assert(endedOutput.head.ended.isDefined)

    // running another of the same output should blank out start/end
    RunStatus.insert(testStage, "o")
    val updatedOutput = RunStatus.of(testStage).filter(_.output == "o")
    assert(updatedOutput.length == 1)
    assert(updatedOutput.head.started.isEmpty)
    assert(updatedOutput.head.ended.isEmpty)

    // clean up
    RunStatus.delete(testStage, "o")
    assert(RunStatus.all().isEmpty)
  }
}
