package org.broadinstitute.dig.aggregator.pipeline.overlapregions

import org.broadinstitute.dig.aggregator.core.Processor
import org.broadinstitute.dig.aggregator.pipeline.Pipeline

/** Registered processors for the OverlappedRegions pipeline.
  */
object OverlapRegionsPipeline extends Pipeline {
  import Processor.Name
  import Processor.register

  /** Register all processors.
    */
  val overlapRegionsProcessor: Name = register("OverlapRegionsProcessor", new OverlapRegionsProcessor(_, _))
  val uploadRegionsProcessor: Name  = register("UploadRegionsProcessor", new UploadOverlapRegionsProcessor(_, _))
}