package org.broadinstitute.dig.aggregator.pipeline.varianteffect

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws.JobStep
import org.broadinstitute.dig.aws.emr.ClusterDef

/** Finds all the variants in a dataset across all phenotypes and writes them
  * out to a set of files that can have VEP run over in parallel.
  *
  * VEP input files written to:
  *
  *  s3://dig-analysis-data/out/varianteffect/variants/<dataset>
  */
class VariantListStage(implicit context: Context) extends Stage {
  val variants: Input.Source = Input.Source.Dataset("variants/")

  /** Source inputs. */
  override val sources: Seq[Input.Source] = Seq(variants)

  /* Define settings for the cluster to run the job.
   */
  override val cluster: ClusterDef = super.cluster.copy(
    masterVolumeSizeInGB = 400,
    slaveVolumeSizeInGB = 400,
  )

  /** Map inputs to outputs. */
  override val rules: PartialFunction[Input, Outputs] = {
    case variants() => Outputs.Named("VEP/variants")
  }

  /** All that matters is that there are new datasets. The input datasets are
    * actually ignored, and _everything_ is reprocessed. This is done because
    * there is only a single analysis node for all variants.
    */
  override def make(output: String): Seq[JobStep] = {
    val pyScript = resourceURI("pipeline/varianteffect/listVariants.py")

    Seq(JobStep.PySpark(pyScript))
  }
}
