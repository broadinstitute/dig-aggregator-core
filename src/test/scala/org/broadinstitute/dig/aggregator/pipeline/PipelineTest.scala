package org.broadinstitute.dig.aggregator.pipeline

import org.scalatest.FunSuite
import org.rogach.scallop.exceptions.ScallopException

final class PipelineTest extends FunSuite {

  test("pipeline - unique processors") {

    /*
     * Lookup some pipelines, which should auto-register them and confirm that
     * there are no duplicate processor names.
     */

    assert(Pipeline("VariantEffectPipeline").isDefined)
    assert(Pipeline("MetaAnalysisPipeline").isDefined)
    assert(Pipeline("FrequencyAnalysisPipeline").isDefined)
    assert(Pipeline("LDClumpingPipeline").isDefined)
  }

  test("pipeline - list processors") {
    import metaanalysis.MetaAnalysisPipeline

    val metaanalysisPipeline   = Pipeline("MetaAnalysisPipeline").get
    val metaanalysisProcessors = metaanalysisPipeline.processors
    val expectedProcessors = Set(
      MetaAnalysisPipeline.metaAnalysisProcessor,
      MetaAnalysisPipeline.uploadMetaAnalysisProcessor
    )

    assert(metaanalysisProcessors == expectedProcessors)
  }
}
