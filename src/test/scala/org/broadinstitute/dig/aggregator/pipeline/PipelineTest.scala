package org.broadinstitute.dig.aggregator.pipeline

import org.scalatest.FunSuite
import org.rogach.scallop.exceptions.ScallopException

final class PipelineTest extends FunSuite {

  test("pipeline - unique processors") {

    /*
     * Lookup some pipelines, which should auto-register them and confirm that
     * there are no duplicate processor names.
     */

    assert(Pipeline("LDscore").isDefined)
    assert(Pipeline("MetaAnalysis").isDefined)
  }

  test("pipeline - list processors") {
    import metaanalysis.MetaAnalysisPipeline

    val metaanalysisPipeline   = Pipeline("MetaAnalysis").get
    val metaanalysisProcessors = metaanalysisPipeline.processors
    val expectedProcessors = Set(
      MetaAnalysisPipeline.variantPartitionProcessor,
      MetaAnalysisPipeline.metaAnalysisProcessor,
      MetaAnalysisPipeline.uploadMetaAnalysisProcessor,
    )

    assert(metaanalysisProcessors == expectedProcessors)
  }
}
