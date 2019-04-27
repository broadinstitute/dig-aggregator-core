package org.broadinstitute.dig.aggregator.pipeline.gregor

import org.broadinstitute.dig.aggregator.core.config.BaseConfig
import org.broadinstitute.dig.aggregator.core.processors.Processor
import org.broadinstitute.dig.aggregator.pipeline.Pipeline

/** Registered processors for the Gregor pipeline.
  */
object GregorPipeline extends Pipeline {
  import Processor.Name
  import Processor.register

  /** Register all processors.
    */
  val sortRegionsProcessor: Name = register("SortRegionsProcessor", new SortRegionsProcessor(_, _))
  val snpListProcessor: Name     = register("SNPListProcessor", new SNPListProcessor(_, _))
  val gregorProcessor: Name      = register("GregorProcessor", new GregorProcessor(_, _))
  //val uploadGregorProcessor: Name = register("UploadGregorProcessor", new UploadGregorProcessor(_, _))
}
