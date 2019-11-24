package org.broadinstitute.dig.aggregator.pipeline.bioindex

import org.broadinstitute.dig.aggregator.core.Processor
import org.broadinstitute.dig.aggregator.pipeline.Pipeline

/** Registered processors for the FrequencyAnalysis pipeline.
  */
object BioIndexPipeline extends Pipeline {
  import Processor.Name
  import Processor.register

  /** Register all intake processors.
    */
  val bioIndexProcessor: Name = register("BioIndexProcessor", new BioIndexProcessor(_, _, _))
}
