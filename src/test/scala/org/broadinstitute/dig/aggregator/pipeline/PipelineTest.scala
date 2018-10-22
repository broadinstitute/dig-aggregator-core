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
    import intake.IntakePipeline

    val intakePipeline   = Pipeline("intake").get
    val intakeProcessors = intakePipeline.processors
    val expectedProcessors = Set(
      IntakePipeline.variantProcessor,
      IntakePipeline.commitProcessor,
      IntakePipeline.thousandGenomeProcessor,
    )

    assert(intakeProcessors == expectedProcessors)
  }
}
