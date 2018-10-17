package org.broadinstitute.dig.aggregator.pipeline

import org.scalatest.FunSuite
import org.rogach.scallop.exceptions.ScallopException

final class PipelineTest extends FunSuite {

  test("pipeline - unique processors") {

    /*
     * Lookup some pipelines, which should auto-register them and confirm that
     * there are no duplicate processor names.
     */

    assert(Pipeline("intake").isDefined)
    assert(Pipeline("metaanalysis").isDefined)
  }

  test("pipeline - list processors") {
    val intakeProcessors = Pipeline("intake").get.processors
    val expectedProcessors = Set(
      "VariantProcessor",
      "CommitProcessor",
      "ThousandGenomeProcessor",
    )

    assert(intakeProcessors == expectedProcessors)
  }
}
