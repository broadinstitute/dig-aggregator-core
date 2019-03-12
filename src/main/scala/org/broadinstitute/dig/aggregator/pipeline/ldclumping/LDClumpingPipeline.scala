package org.broadinstitute.dig.aggregator.pipeline.ldclumping

import org.broadinstitute.dig.aggregator.core.config.BaseConfig
import org.broadinstitute.dig.aggregator.core.processors.Processor
import org.broadinstitute.dig.aggregator.pipeline.Pipeline

/**
 * All processors for the meta-analysis pipeline.
 */
object LDClumpingPipeline extends Pipeline {
  import Processor.Name
  import Processor.register

  /** LD Score processors. */
  val ldClumpingProcessor: Name = register("LDClumpingProcessor", new LDClumpingProcessor(_, _))
  //val ldScoreUploadProcessor  = new Name("LDScoreUploadProcessor", new LDScoreUploadProcessor(_))
}
