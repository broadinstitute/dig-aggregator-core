package org.broadinstitute.dig.aggregator.core

import cats.data.NonEmptyList
import java.util.UUID

/**
  * @author clint
  * Aug 28, 2018
  */
final class RunTest extends DbFunSuite {

  def makeInput(): UUID = UUID.randomUUID()

  dbTest("insert") {
    assert(allResults.isEmpty)

    insertRun(TestProcessor.a, "o0", NonEmptyList(makeInput(), Nil))

    assert(allResults.size == 1)

    insertRun(TestProcessor.b, "o1", NonEmptyList(makeInput(), Nil))
    insertRun(TestProcessor.b, "o2", NonEmptyList(makeInput(), Nil))

    assert(allResults.size == 3)
  }

  dbTest("insert - multiple inputs") {
    val inputs = NonEmptyList(makeInput(), List(makeInput(), makeInput()))
    val r0     = insertRun(TestProcessor.a, "o0", inputs)

    // there should be 3 rows inserted - 1 per input
    assert(runResults(r0).size == 3)
  }

  dbTest("insert - same input, different outputs") {
    val simpleInputs = NonEmptyList(makeInput(), Nil)

    insertRun(TestProcessor.a, "o0", simpleInputs)
    insertRun(TestProcessor.a, "o1", simpleInputs)

    assert(allResults.size == 2)
  }

  dbTest("lookup work to be done 1") {
    val i0 = makeInput()
    val i1 = makeInput()

    val r0 = insertRun(TestProcessor.a, "o0", NonEmptyList(i0, Nil))
    val r1 = insertRun(TestProcessor.b, "o1", NonEmptyList(i1, Nil))

    // everything has already been processed
    val r2 = insertRun(TestProcessor.c, "o2", NonEmptyList(r0, List(r1)))

    // find all outputs processor c needs to process still (depends on a and b)
    val deps    = Seq(TestProcessor.a, TestProcessor.b)
    val results = Run.resultsOf(pool, deps, TestProcessor.c).unsafeRunSync

    assert(results.isEmpty)
  }

  dbTest("lookup work to be done 2") {
    val i0 = makeInput()
    val i1 = makeInput()
    val i2 = makeInput()
    val i3 = makeInput()

    val r0 = insertRun(TestProcessor.a, "o0", NonEmptyList(i0, Nil))
    val r1 = insertRun(TestProcessor.b, "o1", NonEmptyList(i1, Nil))
    val r2 = insertRun(TestProcessor.b, "o2", NonEmptyList(i2, Nil))

    // only r0 and r1 outputs have been processed
    val r3 = insertRun(TestProcessor.c, "o3", NonEmptyList(r0, List(r1)))

    // update the output of r1 by adding a new input
    val r4 = insertRun(TestProcessor.b, "o1", NonEmptyList(i1, List(i3)))

    // find all that processor c needs to process still (depends on a and b)
    val deps    = Seq(TestProcessor.a, TestProcessor.b)
    val results = Run.resultsOf(pool, deps, TestProcessor.c).unsafeRunSync

    // should need to process the outputs r2 (not done yet) and r4 (and update)
    assert(results.size == 2)
    assert(results.exists(_.uuid == r2))
    assert(results.exists(_.uuid == r4))
  }
}
