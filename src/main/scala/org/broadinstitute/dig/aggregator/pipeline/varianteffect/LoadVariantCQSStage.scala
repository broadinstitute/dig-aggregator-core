package org.broadinstitute.dig.aggregator.pipeline.varianteffect

import org.broadinstitute.dig.aggregator.core.{Stage, Run}
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
class LoadVariantCQSStage extends Stage {

  /** All the processors this processor depends on.
    */
  override val dependencies: Seq[Run.Input.Source] = Seq(
    Run.Input.Source.Success("out/varianteffect/effects/"),
  )

  /** Only a single output for VEP that uses ALL effects.
    */
  override def getOutputs(input: Run.Input): Stage.Outputs = {
    Stage.Outputs.Set("VEP/CQS")
  }

  /** All effect results are combined together, so the results list is ignored.
    */
  override def getJob(output: String): Seq[JobStep] = {
    val scriptUri = resourceURI("pipeline/varianteffect/loadCQS.py")

    Seq(JobStep.PySpark(scriptUri))
  }
}
