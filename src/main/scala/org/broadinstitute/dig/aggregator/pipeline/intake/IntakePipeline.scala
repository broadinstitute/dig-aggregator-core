package org.broadinstitute.dig.aggregator.pipeline.intake

import org.broadinstitute.dig.aggregator.core.config.BaseConfig
import org.broadinstitute.dig.aggregator.core.processors.Processor
import org.broadinstitute.dig.aggregator.pipeline.Pipeline

/**
 * Registered processors for the data intake pipeline.
 */
object IntakePipeline extends Pipeline {
  import Processor.Name
  import Processor.register

  /**
   * Register all intake processors.
   */
  val variantProcessor: Name        = register("VariantProcessor", new VariantProcessor(_, _))
  val commitProcessor: Name         = register("CommitProcessor", new CommitProcessor(_, _))
  val thousandGenomeProcessor: Name = register("ThousandGenomeProcessor", new ThousandGenomeProcessor(_, _))
}
