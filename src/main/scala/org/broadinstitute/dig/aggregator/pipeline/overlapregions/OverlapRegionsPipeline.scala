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
  val overlapRegionsProcessor: Name = register("OverlapRegionsProcessor", new OverlapRegionsProcessor(_, _, _))
  val uniqueOverlapRegionsProcessor: Name =
    register("UniqueOverlapRegionsProcessor", new UniqueOverlapRegionsProcessor(_, _, _))
  val uploadOverlapRegionsProcessor: Name =
    register("UploadOverlapRegionsProcessor", new UploadOverlapRegionsProcessor(_, _, _))
  val createRelationshipsProcessor: Name =
    register("CreateOverlapRegionRelationshipsProcessor", new CreateOverlapRegionRelationshipsProcessor(_, _, _))
}
