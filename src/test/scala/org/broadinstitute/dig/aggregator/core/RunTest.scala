package org.broadinstitute.dig.aggregator.core

import scala.util._

/**
 * @author clint
 * Aug 28, 2018
 */
final class RunTest extends DbFunSuite {
  dbTest("insert") {
    val r0 = Run(run = 0, app = "a0", input = "i0", output = "o0")
    val r1 = Run(run = 1, app = "a1", input = "i1", output = "o1")
    val r2 = Run(run = 2, app = "a2", input = "i2", output = "o2")

    assert(allRuns.isEmpty)

    insert(r0)

    assert(allRuns == Seq(r0))

    insert(r1, r2)

    assert(allRuns.toSet == Set(r0, r1, r2))
  }

  dbTest("insert - on duplicate key update") {
    val r0 = Run(run = 0, app = "a0", input = "i0", output = "o0")
    val r1 = Run(run = 1, app = r0.app, input = r0.input, output = "o1")

    assert(allRuns.isEmpty)

    insert(r0)

    assert(allRuns == Seq(r0))

    insert(r1)

    assert(allRuns.toSet == Set(r1))
  }

  dbTest("lookup work to be done 1") {
    val r0 = Run(run = 0, app = "a0", input = "i0", output = "o0")
    val r1 = Run(run = 1, app = "a1", input = "i1", output = "o1")
    val r2 = Run(run = 2, app = "a2", input = "i2", output = "o2")

    // datasets already processed by this app (b)
    val r3 = Run(run = 3, app = "a3", input = r0.output, output = "o3")
    val r4 = Run(run = 3, app = "a3", input = r1.output, output = "o4")
    val r5 = Run(run = 3, app = "a3", input = r2.output, output = "o5")

    // insert everything
    insert(r0, r1, r2, r3, r4, r5)

    // find all the datasets b needs to process
    val results = Run.results(xa, Seq("a0", "a1", "a2"), "a3").unsafeRunSync

    assert(results.isEmpty)
  }

  dbTest("lookup work to be done 2") {
    val r0 = Run(run = 0, app = "a0", input = "i0", output = "o0")
    val r1 = Run(run = 1, app = "a1", input = "i1", output = "o1")
    val r2 = Run(run = 2, app = "a2", input = "i2", output = "o2")

    // datasets already processed by this app (b)
    val r3 = Run(run = 3, app = "a3", input = r0.output, output = "o3")
    val r4 = Run(run = 3, app = "a3", input = r1.output, output = "o4")

    // replace r1 with a new, updated version
    val r5 = Run(run = 4, app = "a1", input = "i1", output = "o1")

    // insert everything
    insert(r0, r1, r2, r3, r4, r5)

    // find all the datasets b needs to process
    val results = Run.results(xa, Seq("a0", "a1", "a2"), "a3").unsafeRunSync

    // should find 2 datasets that need processing
    assert(results.size == 2)
    assert(results.find(_.app == "a1").isDefined)
    assert(results.find(_.app == "a2").isDefined)
  }
}
