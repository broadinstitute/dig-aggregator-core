package org.broadinstitute.dig.aggregator.core

import org.broadinstitute.dig.aggregator.pipeline._

import scala.util._

/**
 * @author clint
 * Aug 28, 2018
 */
final class RunTest extends DbFunSuite {
  dbTest("insert") {
    assert(allResults.isEmpty)

    val r0 = insertRun(TestProcessor.a, Seq("i0"), output = "o0")

    assert(allResults.size == 1)

    val r1 = insertRun(TestProcessor.b, Seq("i1"), "o1")
    val r2 = insertRun(TestProcessor.b, Seq("i2"), "o2")

    assert(allResults.size == 3)
  }

  dbTest("insert - multiple inputs") {
    val r0 = insertRun(TestProcessor.a, Seq("i0", "i1", "i2"), "o0")

    // there should be 3 rows inserted - 1 per input
    assert(runResults(r0).size == 3)
  }

  dbTest("insert - provenance") {
    val r0 = insertRun(TestProcessor.a, Seq("i0", "i1", "i2"), "o0")

    // there should be 1 provenance row
    val p    = Provenance.thisBuild
    val runP = runProvenance(r0, TestProcessor.a)

    assert(runP.size == 1)
    assert(runP(0) == p)
  }

  dbTest("insert - provenance update") {
    val r0 = insertRun(TestProcessor.a, Seq("i0", "i1", "i2"), "o0")
    val r1 = insertRun(TestProcessor.a, Seq("i0", "i1", "i2"), "o1")

    // there should be 1 provenance row
    val p    = Provenance.thisBuild
    val run0 = runProvenance(r0, TestProcessor.a)
    val run1 = runProvenance(r1, TestProcessor.a)

    assert(run0.size == 0)
    assert(run1.size == 1)

    assert(run1(0) == p)
  }

  dbTest("insert - on duplicate key update") {
    val r0 = insertRun(TestProcessor.a, Seq("i0"), "o0")
    val r1 = insertRun(TestProcessor.a, Seq("i0"), "o1")

    assert(allResults.size == 1)

    var r0results = runResults(r0)
    var r1results = runResults(r1)

    assert(r0results.isEmpty)
    assert(r1results.size == 1)
    assert(r1results(0).output == "o1")
  }

  dbTest("lookup work to be done 1") {
    val r0 = insertRun(TestProcessor.a, Seq("i0"), "o0")
    val r1 = insertRun(TestProcessor.b, Seq("i1"), "o1")

    // everything has already been processed
    val r2 = insertRun(TestProcessor.c, Seq("o0", "o1"), "o2")

    // find all the processor c needs to process still (depends on a and b)
    val deps    = Seq(TestProcessor.a, TestProcessor.b)
    val results = Run.results(xa, deps, TestProcessor.c).unsafeRunSync

    assert(results.isEmpty)
  }

  dbTest("lookup work to be done 2") {
    val r0 = insertRun(TestProcessor.a, Seq("i0"), "o0")
    val r1 = insertRun(TestProcessor.b, Seq("i1"), "o1")
    val r2 = insertRun(TestProcessor.b, Seq("i2"), "o2")

    // only r0 and r1 outputs have been processed
    val r3 = insertRun(TestProcessor.c, Seq("o0", "o1"), "o3")

    // update the output of r1
    val r4 = insertRun(TestProcessor.b, Seq("i1"), "o4")

    // find all the processor c needs to process still (depends on a and b)
    val deps    = Seq(TestProcessor.a, TestProcessor.b)
    val results = Run.results(xa, deps, TestProcessor.c).unsafeRunSync

    // should need to process o4 and o2
    assert(results.size == 2)
    assert(results.find(_.output == "o2").isDefined)
    assert(results.find(_.output == "o4").isDefined)
  }
}
