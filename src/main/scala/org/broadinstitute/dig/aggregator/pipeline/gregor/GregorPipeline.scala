package org.broadinstitute.dig.aggregator.pipeline.gregor

import org.broadinstitute.dig.aggregator.core.Processor
import org.broadinstitute.dig.aggregator.pipeline.Pipeline

/** Registered processors for the Gregor pipeline.
  */
object GregorPipeline extends Pipeline {
  import Processor.Name
  import Processor.register

  /** Register all processors.
    */
  val sortRegionsProcessor: Name      = register("SortRegionsProcessor", new SortRegionsProcessor(_, _, _))
  val snpListProcessor: Name          = register("SNPListProcessor", new SNPListProcessor(_, _, _))
  val globalEnrichmentProcessor: Name = register("GlobalEnrichmentProcessor", new GlobalEnrichmentProcessor(_, _, _))
}
