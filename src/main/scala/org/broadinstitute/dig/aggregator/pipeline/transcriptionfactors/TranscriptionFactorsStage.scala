package org.broadinstitute.dig.aggregator.pipeline.transcriptionfactors

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws.JobStep

class TranscriptionFactorsStage(implicit context: Context) extends Stage {

  /** Input sources. */
  override val sources: Seq[Input.Source] = Seq(
    Input.Source.Dataset("transcription_factors/"),
    Input.Source.Success("out/varianteffect/variants/"),
  )

  /** All transcriptions factors are run across all variants all the time.
    * We use the variant list produced by VEP to do this.
    */
  override val rules: PartialFunction[Input, Outputs] = {
    case _ => Outputs.Named("TranscriptionFactors")
  }

  /** With a new variants list or new regions, need to reprocess and
    * get a list of all regions with the variants that they overlap.
    */
  override def make(output: String): Seq[JobStep] = {
    val script = resourceURI("pipeline/transcriptionfactors/transcriptionFactors.py")

    // there's only a single output that ever needs processed.
    Seq(JobStep.PySpark(script))
  }
}
