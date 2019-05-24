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

    val r0 = insertRun(TestProcessor.a, "o0", Seq("i0"))

    assert(allResults.size == 1)

    val r1 = insertRun(TestProcessor.b, "o1", Seq("i1"))
    val r2 = insertRun(TestProcessor.b, "o2", Seq("i2"))

    assert(allResults.size == 3)
  }

  dbTest("insert - multiple inputs") {
    val r0 = insertRun(TestProcessor.a, "o0", Seq("i0", "i1", "i2"))

    // there should be 3 rows inserted - 1 per input
    assert(runResults(r0).size == 3)
  }

  dbTest("insert - same input, different outputs") {
    val r0 = insertRun(TestProcessor.a, "o0", Seq("i0"))
    val r1 = insertRun(TestProcessor.a, "o1", Seq("i0"))

    assert(allResults.size == 2)
  }

  dbTest("lookup work to be done 1") {
    val r0 = insertRun(TestProcessor.a, "o0", Seq("i0"))
    val r1 = insertRun(TestProcessor.b, "o1", Seq("i1"))

    // everything has already been processed
    val r2 = insertRun(TestProcessor.c, "o2", Seq("o0", "o1"))

    // find all outputs processor c needs to process still (depends on a and b)
    val deps    = Seq(TestProcessor.a, TestProcessor.b)
    val results = Run.resultsOf(pool, deps, Some(TestProcessor.c)).unsafeRunSync

    assert(results.isEmpty)
  }

  dbTest("lookup work to be done 2") {
    val r0 = insertRun(TestProcessor.a, "o0", Seq("i0"))
    val r1 = insertRun(TestProcessor.b, "o1", Seq("i1"))
    val r2 = insertRun(TestProcessor.b, "o2", Seq("i2"))

    // only r0 and r1 outputs have been processed
    val r3 = insertRun(TestProcessor.c, "o3", Seq("o0", "o1"))

    // update the output of r1
    val r4 = insertRun(TestProcessor.b, "o1", Seq("i3"))

    // find all the processor c needs to process still (depends on a and b)
    val deps    = Seq(TestProcessor.a, TestProcessor.b)
    val results = Run.resultsOf(pool, deps, Some(TestProcessor.c)).unsafeRunSync

    // should need to process o4 and o2
    assert(results.size == 2)
    assert(results.exists(_.output == "o2"))
    assert(results.exists(_.output == "o1"))
  }
}
