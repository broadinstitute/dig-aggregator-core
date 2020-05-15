package org.broadinstitute.dig.aggregator.pipeline.varianteffect

import org.broadinstitute.dig.aggregator.core.Processor
import org.broadinstitute.dig.aggregator.core.Run
import org.broadinstitute.dig.aggregator.core.config.BaseConfig
import org.broadinstitute.dig.aggregator.pipeline.intake.IntakePipeline
import org.broadinstitute.dig.aws.JobStep
import org.broadinstitute.dig.aws.emr.{Spark, Cluster, InstanceType}
import cats.effect.IO
import org.broadinstitute.dig.aggregator.core.DbPool

/** Finds all the variants in a dataset across all phenotypes and writes them
  * out to a set of files that can have VEP run over in parallel.
  *
  * VEP input files written to:
  *
  *  s3://dig-analysis-data/out/varianteffect/variants/<dataset>
  */
class VariantListProcessor(name: Processor.Name, config: BaseConfig, pool: DbPool)
    extends Processor(name, config, pool) {

  /** Intake dependencies.
    */
  override val dependencies: Seq[Processor.Name] = Seq(IntakePipeline.variants)

  /** All the job scripts that need to be uploaded to AWS.
    */
  override val resources: Seq[String] = Seq(
    "pipeline/varianteffect/listVariants.py"
  )

  /* Define settings for the cluster to run the job.
   */
  override val cluster: Cluster = super.cluster.copy(
    masterVolumeSizeInGB = 400,
    slaveVolumeSizeInGB = 400,
  )

  /** Only a single output for VEP that uses ALL datasets.
    */
  override def getOutputs(input: Run.Result): Processor.OutputList = {
    Processor.Outputs(Seq("VEP/variants"))
  }

  /** All that matters is that there are new datasets. The input datasets are
    * actually ignored, and _everything_ is reprocessed. This is done because
    * there is only a single analysis node for all variants.
    */
  override def getJob(output: String): Seq[JobStep] = {
    val pyScript = aws.uriOf("resources/pipeline/varianteffect/listVariants.py")

    Seq(JobStep.PySpark(pyScript))
  }
}
