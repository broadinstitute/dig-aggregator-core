package org.broadinstitute.dig.aggregator.core

import org.rogach.scallop.exceptions.ValidationFailure
import org.scalatest.funsuite.AnyFunSuite

final class OptsTest extends AnyFunSuite {

  test("mutually exclusive flags") {
    assertThrows[ValidationFailure] {
      new Opts(Seq("--insert-runs", "--no-insert-runs"))
    }
  }

  test("dry run") {
    assert(!new Opts(Seq("--yes")).dryRun())
    assert(new Opts(Seq.empty).dryRun())
  }

  test("only and exclude") {
    val opts  = new Opts(Seq("--only", "FG*,T2D*", "--exclude", "*adj*"))
    val tests = Seq("BMI", "FG", "FGadjBMI", "T2D", "T2DadjBMI", "FI", "WHR")

    // get the only and excluding tests
    val onlyTests    = tests.filter(test => opts.onlyGlobs.get.exists(_.matches(test)))
    val excludeTests = tests.filter(test => opts.excludeGlobs.get.exists(_.matches(test)))

    // ensure sets match
    assert(onlyTests.toSet == Set("FG", "FGadjBMI", "T2D", "T2DadjBMI"))
    assert(excludeTests.toSet == Set("FGadjBMI", "T2DadjBMI"))

    // remove tests from the only matching tests
    val finalTests = onlyTests.filterNot(test => opts.excludeGlobs.get.exists(_.matches(test)))

    // final set
    assert(finalTests.toSet == Set("FG", "T2D"))
  }
}
