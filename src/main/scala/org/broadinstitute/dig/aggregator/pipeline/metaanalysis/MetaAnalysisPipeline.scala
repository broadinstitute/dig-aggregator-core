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
  val variantPartitionProcessor: Name = register("VariantPartitionProcessor", new VariantPartitionProcessor(_, _))
  val ancestrySpecificProcessor: Name = register("AncestrySpecificProcessor", new AncestrySpecificProcessor(_, _))
  val transEthnicProcessor: Name      = register("TransEthnicProcessor", new TransEthnicProcessor(_, _))
  val frequencyUploadProcessor: Name  = register("FrequencyUploadProcessor", new FrequencyUploadProcessor(_, _))
  val bottomLineUploadProcessor: Name = register("BottomLineUploadProcessor", new BottomLineUploadProcessor(_, _))
}
