package org.broadinstitute.dig.aggregator.pipeline.metaanalysis

import org.broadinstitute.dig.aggregator.core.config.BaseConfig
import org.broadinstitute.dig.aggregator.core.processors.Processor
import org.broadinstitute.dig.aggregator.pipeline.Pipeline

/**
 * Registered processors for the MetaAnalysis pipeline.
 */
object MetaAnalysisPipeline extends Pipeline {
  import Processor.Name
  import Processor.register

  /**
   * Register all intake processors.
   */
  val metaAnalysisProcessor: Name       = register("MetaAnalysisProcessor", new MetaAnalysisProcessor(_, _))
  val uploadMetaAnalysisProcessor: Name = register("UploadMetaAnalysisProcessor", new UploadMetaAnalysisProcessor(_, _))
}
