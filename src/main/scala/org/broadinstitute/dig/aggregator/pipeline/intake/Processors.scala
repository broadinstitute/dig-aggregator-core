package org.broadinstitute.dig.aggregator.pipeline.intake

import org.broadinstitute.dig.aggregator.pipeline.ProcessorRegister

/**
 * All processors for the intake pipeline.
 */
object Processors extends ProcessorRegister {
  import org.broadinstitute.dig.aggregator.core.processors.Processor.Name

  /** Intake processors. */
  val variantProcessor = new Name("VariantProcessor", new VariantProcessor(_))
  val commitProcessor  = new Name("CommitProcessor", new CommitProcessor(_))
}
