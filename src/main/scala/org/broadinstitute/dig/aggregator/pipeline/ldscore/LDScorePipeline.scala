package org.broadinstitute.dig.aggregator.pipeline.ldscore

import org.broadinstitute.dig.aggregator.core.config.BaseConfig
import org.broadinstitute.dig.aggregator.core.processors.Processor
import org.broadinstitute.dig.aggregator.pipeline.Pipeline

/**
 * All processors for the meta-analysis pipeline.
 */
object LDScorePipeline extends Pipeline {
  import Processor.Name
  import Processor.register

  /** LD Score processors. */
  val ldScoreRegressionProcessor: Name = register("LDScoreRegressionProcessor", new LDScoreRegressionProcessor(_, _))
  //val ldScoreUploadProcessor  = new Name("LDScoreUploadProcessor", new LDScoreUploadProcessor(_))
}
