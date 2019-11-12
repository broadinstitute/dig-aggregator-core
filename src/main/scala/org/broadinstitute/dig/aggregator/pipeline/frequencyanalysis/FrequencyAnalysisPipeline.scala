package org.broadinstitute.dig.aggregator.pipeline.frequencyanalysis

import org.broadinstitute.dig.aggregator.core.Processor
import org.broadinstitute.dig.aggregator.core.config.BaseConfig
import org.broadinstitute.dig.aggregator.pipeline.Pipeline

/**
  * Registered processors for the FrequencyAnalysis pipeline.
  */
object FrequencyAnalysisPipeline extends Pipeline {
  import Processor.Name
  import Processor.register

  /**
    * Register all intake processors.
    */
  val frequencyProcessor: Name = register("FrequencyAnalysisProcessor", new FrequencyAnalysisProcessor(_, _, _))
  val uploadFrequencyProcessor: Name =
    register("UploadFrequencyAnalysisProcessor", new UploadFrequencyAnalysisProcessor(_, _, _))
}
