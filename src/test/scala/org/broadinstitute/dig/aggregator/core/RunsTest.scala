package org.broadinstitute.dig.aggregator.core

import java.util.UUID

import org.scalatest.funsuite.AnyFunSuite

final class RunsTest extends AnyFunSuite with ProvidesH2Transactor {

  // used to create method context
  implicit val testOpts: Opts = new Opts(Seq("--test"))
  implicit val testDb: DbPool = pool

  // create a dummy input
  def input(name: String): Input = Input(name, UUID.randomUUID.toString)

  test("migrate") {
    Context.use(TestMethod) {
      Runs.migrate()
      assert(Runs.all() == 0)
    }
  }

  test("insert/delete - single input") {
    Context.use(TestMethod) {
      Runs.migrate()

      // insert a single run
      Runs.insert(TestMethod.TestStage, "o1", Seq(input("i1")))
      assert(Runs.all().size == 1)

      // delete run
      Runs.delete(TestMethod.TestStage, "o1")
      assert(Runs.all().isEmpty)
    }
  }

  test("insert/delete - multiple inputs/outputs") {
    val inputs = (1 to 6).map(_.toString).map(input)

    Context.use(TestMethod) {
      Runs.migrate()

      // insert multiple inputs for a single output
      Runs.insert(TestMethod.TestStage, "o1", inputs.take(3))
      Runs.insert(TestMethod.TestStage, "o2", inputs.drop(3))

      // ensure the inputs match outputs
      val results = Runs.of(TestMethod.TestStage)

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
      Runs.delete(TestMethod.TestStage, "o1")
      Runs.delete(TestMethod.TestStage, "o2")
      assert(Runs.all().isEmpty)
    }
  }

  test("update output with changed inputs") {
    val inputs        = (1 to 6).map(_.toString).map(input)
    val updatedInputs = inputs.map(_.copy(eTag = UUID.randomUUID.toString))

    Context.use(TestMethod) {
      Runs.migrate()

      // insert a single output with 3 outputs
      Runs.insert(TestMethod.TestStage, "o", inputs.take(3))

      // get all the inputs and verify them
      val i1 = Runs.all().map(r => r.input -> r.version).map((Input.apply _).tupled).toSet
      assert(i1 == inputs.take(3).toSet)

      // insert the same output with 3 new outputs
      Runs.insert(TestMethod.TestStage, "o", inputs.drop(3))

      // get the new inputs and verify them
      val i2 = Runs.all().map(r => r.input -> r.version).map((Input.apply _).tupled).toSet
      assert(i2 == inputs.toSet)

      // insert the updated inputs (same key, different version)
      Runs.insert(TestMethod.TestStage, "o", updatedInputs)

      // get the new inputs and verify them
      val i3 = Runs.all().map(r => r.input -> r.version).map((Input.apply _).tupled).toSet
      assert(i3 == updatedInputs.toSet)

      // clean up
      Runs.delete(TestMethod.TestStage, "o")
      assert(Runs.all().isEmpty)
    }
  }
}
