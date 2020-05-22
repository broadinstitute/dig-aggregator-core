package org.broadinstitute.dig.aggregator.pipeline.varianteffect

import org.broadinstitute.dig.aggregator.core.{Stage, Input, Outputs}
import org.broadinstitute.dig.aws.JobStep
import org.broadinstitute.dig.aws.emr.ClusterDef

/** Finds all the variants in a dataset across all phenotypes and writes them
  * out to a set of files that can have VEP run over in parallel.
  *
  * VEP input files written to:
  *
  *  s3://dig-analysis-data/out/varianteffect/variants/<dataset>
  */
class VariantListStage extends Stage {

  /** Intake dependencies.
    */
  override val dependencies: Seq[Input.Source] = Seq(
    Input.Source.Dataset("variants/"),
  )

  /* Define settings for the cluster to run the job.
   */
  override val cluster: ClusterDef = super.cluster.copy(
    masterVolumeSizeInGB = 400,
    slaveVolumeSizeInGB = 400,
  )

  /** Only a single output for VEP that uses ALL datasets.
    */
  override def getOutputs(input: Input): Outputs = {
    Outputs.Named("VEP/variants")
  }

  /** All that matters is that there are new datasets. The input datasets are
    * actually ignored, and _everything_ is reprocessed. This is done because
    * there is only a single analysis node for all variants.
    */
  override def getJob(output: String): Seq[JobStep] = {
    val pyScript = resourceURI("pipeline/varianteffect/listVariants.py")

    Seq(JobStep.PySpark(pyScript))
  }
}
