package org.broadinstitute.dig.aggregator.pipeline.varianteffect

import org.broadinstitute.dig.aggregator.core.config.BaseConfig
import org.broadinstitute.dig.aggregator.core.processors.Processor
import org.broadinstitute.dig.aggregator.pipeline.Pipeline

/**
 * Registered processors for the MetaAnalysis pipeline.
 */
object VariantEffectPipeline extends Pipeline {
  import Processor.Name
  import Processor.register

  /**
   * Register all intake processors.
   */
  val variantEffectProcessor: Name = register("VariantEffectProcessor", new VariantEffectProcessor(_, _))
  val uploadEffectProcessor: Name  = register("UploadEffectProcessor", new UploadEffectProcessor(_, _))
}
