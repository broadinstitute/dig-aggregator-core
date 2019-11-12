package org.broadinstitute.dig.aggregator.pipeline.varianteffect

import org.broadinstitute.dig.aggregator.core.Processor
import org.broadinstitute.dig.aggregator.core.config.BaseConfig
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
  val variantListProcessor: Name      = register("VariantListProcessor", new VariantListProcessor(_, _, _))
  val variantEffectProcessor: Name    = register("VariantEffectProcessor", new VariantEffectProcessor(_, _, _))
  val loadVariantCQSProcessor: Name   = register("LoadVariantCQSProcessor", new LoadVariantCQSProcessor(_, _, _))
  val uploadVariantCQSProcessor: Name = register("UploadVariantCQSProcessor", new UploadVariantCQSProcessor(_, _, _))
}
