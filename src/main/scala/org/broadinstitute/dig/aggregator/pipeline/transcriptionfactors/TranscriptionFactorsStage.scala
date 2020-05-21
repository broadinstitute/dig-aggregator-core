package org.broadinstitute.dig.aggregator.pipeline.transcriptionfactors

import org.broadinstitute.dig.aggregator.core.{Run, Stage}
import org.broadinstitute.dig.aws.JobStep

class TranscriptionFactorsStage extends Stage {

  /** Dependency processors.
    */
  override val dependencies: Seq[Run.Input.Source] = Seq(
    Run.Input.Source.Dataset("transcription_factors/"),
    Run.Input.Source.Success("out/varianteffect/variants/"),
  )

  /** All transcriptions factors are run across all variants all the time.
    * We use the variant list produced by VEP to do this.
    */
  override def getOutputs(input: Run.Input): Stage.Outputs = {
    Stage.Outputs.Set("TranscriptionFactors")
  }

  /** With a new variants list or new regions, need to reprocess and
    * get a list of all regions with the variants that they overlap.
    */
  override def getJob(output: String): Seq[JobStep] = {
    val script = resourceURI("pipeline/transcriptionfactors/transcriptionFactors.py")

    // there's only a single output that ever needs processed.
    Seq(JobStep.PySpark(script))
  }
}
