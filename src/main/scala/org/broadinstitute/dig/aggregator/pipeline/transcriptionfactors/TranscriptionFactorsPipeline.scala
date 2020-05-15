package org.broadinstitute.dig.aggregator.pipeline.transcriptionfactors

import org.broadinstitute.dig.aggregator.core.Processor
import org.broadinstitute.dig.aggregator.pipeline.Pipeline

/** Registered processors for the OverlappedRegions pipeline.
  */
object TranscriptionFactorsPipeline extends Pipeline {
  import Processor.Name
  import Processor.register

  /** Register all processors.
    */
  val transcriptionFactorsProcessor: Name =
    register("TranscriptionFactorsProcessor", new TranscriptionFactorsProcessor(_, _, _))
}
