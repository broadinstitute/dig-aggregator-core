package org.broadinstitute.dig.aggregator.pipeline.varianteffect

import cats.effect._

import java.util.UUID

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aggregator.core.config.BaseConfig
import org.broadinstitute.dig.aggregator.core.emr._

/** After all the variants across all datasets have had VEP run on them in the
  * previous step, the results must be joined together. This is done by loading
  * all the resulting JSON files together, and only keeping a single output
  * per variant ID; the results of VEP are by variants and so will be identical
  * across datasets.
  *
  * The input location:
  *
  *  s3://dig-analysis-data/out/varianteffect/effects/<dataset>/<phenotype>/part-*.json
  *
  * The output location:
  *
  *  s3://dig-analysis-data/out/varianteffect/transcript_consequences/part-*.csv
  *  s3://dig-analysis-data/out/varianteffect/regulatory_features/part-*.csv
  *
  * The inputs and outputs for this processor are expected to be phenotypes.
  */
class LoadVariantCQSProcessor(name: Processor.Name, config: BaseConfig) extends Processor(name, config) {

  /** All the processors this processor depends on.
    */
  override val dependencies: Seq[Processor.Name] = Seq(
    VariantEffectPipeline.variantEffectProcessor
  )

  /** All the job scripts that need to be uploaded to AWS.
    */
  override val resources: Seq[String] = Seq(
    "pipeline/varianteffect/loadCQS.py"
  )

  /** Only a single output for VEP that uses ALL effects.
    */
  override def getOutputs(input: Run.Result): Processor.OutputList = {
    Processor.Outputs(Seq("VEP/CQS"))
  }

  /** All effect results are combined together, so the results list is ignored.
    */
  override def processOutputs(outputs: Seq[String]): IO[Unit] = {
    val scriptUri = aws.uriOf("resources/pipeline/varianteffect/loadCQS.py")
    val sparkConf = ApplicationConfig.sparkEnv.withConfig(ClassificationProperties.sparkUsePython3)

    // EMR cluster to run the job steps on
    val cluster = Cluster(
      name = name.toString,
      instances = 5,
      configurations = Seq(sparkConf)
    )

    // first run+load ancestry-specific and then trans-ethnic
    val steps = Seq(JobStep.PySpark(scriptUri))

    for {
      _   <- IO(logger.info(s"Loading variant consequences..."))
      job <- aws.runJob(cluster, steps)
      _   <- aws.waitForJob(job)
    } yield ()
  }
}
