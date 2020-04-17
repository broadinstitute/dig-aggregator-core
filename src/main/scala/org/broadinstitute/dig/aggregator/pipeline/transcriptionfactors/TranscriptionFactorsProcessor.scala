package org.broadinstitute.dig.aggregator.pipeline.transcriptionfactors

import cats.effect.IO
import org.broadinstitute.dig.aggregator.core.{Processor, Run}
import org.broadinstitute.dig.aggregator.core.config.BaseConfig
import org.broadinstitute.dig.aggregator.pipeline.intake.IntakePipeline
import org.broadinstitute.dig.aggregator.pipeline.varianteffect.VariantEffectPipeline
import org.broadinstitute.dig.aws.JobStep
import org.broadinstitute.dig.aws.emr.{Cluster, InstanceType, Spark}
import org.broadinstitute.dig.aggregator.core.DbPool

class TranscriptionFactorsProcessor(name: Processor.Name, config: BaseConfig, pool: DbPool)
    extends Processor(name, config, pool) {

  /** Dependency processors.
    */
  override val dependencies: Seq[Processor.Name] = Seq(
    IntakePipeline.transcriptionFactors,
    VariantEffectPipeline.variantListProcessor,
  )

  /** All the job scripts that need to be uploaded to AWS.
    */
  override val resources: Seq[String] = Seq(
    "pipeline/transcriptionfactors/transcriptionFactors.py",
  )

  /** All transcriptions factors are run across all variants all the time. We use the
    * variant list produced by VEP to do this.
    */
  override def getOutputs(input: Run.Result): Processor.OutputList = {
    Processor.Outputs(Seq("TranscriptionFactors"))
  }

  /** With a new variants list or new regions, need to reprocess and get a list
    * of all regions with the variants that they overlap.
    */
  override def processOutputs(outputs: Seq[String]): IO[Unit] = {
    val script = aws.uriOf("resources/pipeline/transcriptionfactors/transcriptionFactors.py")

    // cluster configuration used to process each phenotype
    val cluster = Cluster(
      name = name.toString,
      masterInstanceType = InstanceType.c5_4xlarge,
      slaveInstanceType = InstanceType.c5_2xlarge,
      instances = 5,
      configurations = Seq(
        Spark.Env.config.withProperty(Spark.Env.Export.usePython3),
      )
    )

    // there's only a single output that ever needs processed.
    aws.runJob(cluster, JobStep.PySpark(script))
  }
}
