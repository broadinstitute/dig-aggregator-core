package org.broadinstitute.dig.aggregator.pipeline.varianteffect

import cats.effect._

import java.util.UUID

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aggregator.core.config.BaseConfig
import org.broadinstitute.dig.aggregator.core.emr._
import org.broadinstitute.dig.aggregator.pipeline.intake.IntakePipeline

/** Finds all the variants in a dataset across all phenotypes and writes them
  * out to a set of files that can have VEP run over in parallel.
  *
  * VEP input files written to:
  *
  *  s3://dig-analysis-data/out/varianteffect/variants/<dataset>
  */
class VariantListProcessor(name: Processor.Name, config: BaseConfig) extends Processor(name, config) {

  /** Intake dependencies.
    */
  override val dependencies: Seq[Processor.Name] = Seq(IntakePipeline.variants)

  /** All the job scripts that need to be uploaded to AWS.
    */
  override val resources: Seq[String] = Seq(
    "pipeline/varianteffect/listVariants.py"
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
  override def processOutputs(outputs: Seq[String]): IO[Unit] = {
    val pyScript = aws.uriOf("resources/pipeline/varianteffect/listVariants.py")

    // spark configuration settings
    val sparkConf = ApplicationConfig.sparkEnv.withConfig(ClassificationProperties.sparkUsePython3)

    // define settings for the cluster to run the job
    val cluster = Cluster(
      name = name.toString,
      instances = 5,
      configurations = Seq(sparkConf)
    )

    for {
      _   <- IO(logger.info("Processing datasets..."))
      job <- aws.runJob(cluster, JobStep.PySpark(pyScript))
      _   <- aws.waitForJob(job)
    } yield ()
  }
}
