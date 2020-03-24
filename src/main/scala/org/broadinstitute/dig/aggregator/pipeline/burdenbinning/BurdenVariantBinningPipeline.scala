package org.broadinstitute.dig.aggregator.pipeline.burdenbinning

import org.broadinstitute.dig.aggregator.core.Processor
import org.broadinstitute.dig.aggregator.pipeline.Pipeline

/** Registered processors for the Burden variant binning pipeline.
  */
object BurdenVariantBinningPipeline extends Pipeline {
  import Processor.{Name, register}

  /** Register all intake processors.
    */
  val burdenVariantBinningProcessor: Name       = register("BurdenVariantBinningProcessor", new BurdenVariantBinningProcessor(_, _, _))
}
