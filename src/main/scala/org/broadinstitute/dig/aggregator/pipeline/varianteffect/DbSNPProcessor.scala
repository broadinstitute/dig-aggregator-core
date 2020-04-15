package org.broadinstitute.dig.aggregator.pipeline.varianteffect

import org.broadinstitute.dig.aggregator.core.Processor
import org.broadinstitute.dig.aggregator.core.Run
import org.broadinstitute.dig.aggregator.core.config.BaseConfig
import org.broadinstitute.dig.aws.JobStep
import org.broadinstitute.dig.aws.emr.{ApplicationConfig, ClassificationProperties, Cluster, InstanceType}
import cats.effect.IO
import org.broadinstitute.dig.aggregator.core.DbPool

/** After all the variants across all datasets have had VEP run on them in the
  * previous step the rsID for each variant is extracted into its own file.
  *
  * The input location:
  *
  *  s3://dig-analysis-data/out/varianteffect/effects/part-*.json
  *
  * The output location:
  *
  *  s3://dig-analysis-data/out/varianteffect/dbsnp/part-*.csv
  *
  * The inputs and outputs for this processor are expected to be phenotypes.
  */
class DbSNPProcessor(name: Processor.Name, config: BaseConfig, pool: DbPool) extends Processor(name, config, pool) {

  /** All the processors this processor depends on.
    */
  override val dependencies: Seq[Processor.Name] = Seq(
    VariantEffectPipeline.variantEffectProcessor
  )

  /** All the job scripts that need to be uploaded to AWS.
    */
  override val resources: Seq[String] = Seq(
    "pipeline/varianteffect/dbSNP.py"
  )

  /** Only a single output for VEP that uses ALL effects.
    */
  override def getOutputs(input: Run.Result): Processor.OutputList = {
    Processor.Outputs(Seq("VEP/SNP"))
  }

  /** All effect results are combined together, so the results list is ignored.
    */
  override def processOutputs(outputs: Seq[String]): IO[Unit] = {
    val scriptUri = aws.uriOf("resources/pipeline/varianteffect/dbSNP.py")

    // EMR cluster to run the job steps on
    val cluster = Cluster(
      name = name.toString,
      masterInstanceType = InstanceType.c5_9xlarge,
      slaveInstanceType = InstanceType.c5_9xlarge,
      instances = 4,
      configurations = Seq(
        ApplicationConfig.sparkEnv.withConfig(ClassificationProperties.sparkUsePython3),
        ApplicationConfig.sparkMaximizeResourceAllocation,
      ),
    )

    // first run+load ancestry-specific and then trans-ethnic
    val steps = Seq(JobStep.PySpark(scriptUri))

    for {
      job <- aws.runJob(cluster, steps)
      _   <- aws.waitForJob(job)
    } yield ()
  }
}
