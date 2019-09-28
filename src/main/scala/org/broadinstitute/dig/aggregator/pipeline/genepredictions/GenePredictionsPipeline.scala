package org.broadinstitute.dig.aggregator.pipeline.genepredictions

import org.broadinstitute.dig.aggregator.core.Processor
import org.broadinstitute.dig.aggregator.pipeline.Pipeline

/** Registered processors for the gene predictions pipeline.
  */
object GenePredictionsPipeline extends Pipeline {
  import Processor.Name
  import Processor.register

  /** Register all processors.
    */
  val genePredictionsProcessor: Name = register("GenePredictionsProcessor", new GenePredictionsProcessor(_, _))
  val uploadGenePredictionsProcessor: Name =
    register("UploadGenePredictionsProcessor", new UploadGenePredictionsProcessor(_, _))
}
