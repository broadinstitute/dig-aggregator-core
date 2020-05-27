package org.broadinstitute.dig.aggregator.pipeline.varianteffect

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws.JobStep

/** After all the variants across all datasets have had VEP run on them in the
  * previous step, the results must be joined together. This is done by loading
  * all the resulting JSON files together, and only keeping a single output
  * per variant ID; the results of VEP are by variants and so will be identical
  * across datasets.
  *
  * The input location:
  *
  *  s3://dig-analysis-data/out/varianteffect/effects/part-*.json
  *
  * The output location:
  *
  *  s3://dig-analysis-data/out/varianteffect/transcript_consequences/part-*.csv
  *  s3://dig-analysis-data/out/varianteffect/regulatory_features/part-*.csv
  *
  * The inputs and outputs for this processor are expected to be phenotypes.
  */
class LoadVariantCQSStage(implicit context: Context) extends Stage {
  val effects: Input.Source = Input.Source.Success("out/varianteffect/effects/")

  /** Input sources. */
  override val sources: Seq[Input.Source] = Seq(effects)

  /** Mapping inputs to outputs. */
  override val rules: PartialFunction[Input, Outputs] = {
    case effects() => Outputs.Named("VEP/CQS")
  }

  /** All effect results are combined together, so the results list is ignored. */
  override def make(output: String): Seq[JobStep] = {
    val scriptUri = resourceUri("pipeline/varianteffect/loadCQS.py")

    Seq(JobStep.PySpark(scriptUri))
  }
}
