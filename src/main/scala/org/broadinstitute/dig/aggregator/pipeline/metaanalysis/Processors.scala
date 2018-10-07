package org.broadinstitute.dig.aggregator.pipeline.metaanalysis

import org.broadinstitute.dig.aggregator.pipeline.ProcessorRegister

/**
 * All processors for the meta-analysis pipeline.
 */
object Processors extends ProcessorRegister {
  import org.broadinstitute.dig.aggregator.core.processors.Processor.Name

  /** Meta-Analysis processors. */
  val variantPartitionProcessor = new Name("VariantPartitionProcessor", new VariantPartitionProcessor(_))
  val ancestrySpecificProcessor = new Name("AncestrySpecificProcessor", new AncestrySpecificProcessor(_))
  val transEthnicProcessor      = new Name("TransEthnicProcessor", new TransEthnicProcessor(_))
}
