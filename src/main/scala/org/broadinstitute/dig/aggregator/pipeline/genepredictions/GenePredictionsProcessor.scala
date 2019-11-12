package org.broadinstitute.dig.aggregator.pipeline.genepredictions

import cats.effect.IO
import org.broadinstitute.dig.aggregator.core.{Processor, Run}
import org.broadinstitute.dig.aggregator.core.config.BaseConfig
import org.broadinstitute.dig.aggregator.pipeline.intake.IntakePipeline
import org.broadinstitute.dig.aws.JobStep
import org.broadinstitute.dig.aws.emr.{ApplicationConfig, ClassificationProperties, Cluster, InstanceType}
import org.broadinstitute.dig.aggregator.core.DbPool

/** Gene Prediction regions are joined with the uploaded gene list for the
  * proper assembly and then written out to be uploaded.
  *
  * Input HDFS data:
  *
  *  s3://dig-analysis-data/gene_predictions/<dataset>/part-*
  *  s3://dig-analysis-data/genes/GRChXX/part-*
  *
  * Output HDFS results:
  *
  *  s3://dig-analysis-data/out/genepredictions/<dataset>part-*
  */
class GenePredictionsProcessor(name: Processor.Name, config: BaseConfig, pool: DbPool) extends Processor(name, config, pool) {

  /** Source data to consume.
    */
  override val dependencies: Seq[Processor.Name] = Seq(
    IntakePipeline.genes,
    IntakePipeline.genePredictions,
  )

  /** All the job scripts that need to be uploaded to AWS.
    */
  override val resources: Seq[String] = Seq(
    "pipeline/genepredictions/genePredictions.py"
  )

  /** Always reprocess everything.
    */
  override def getOutputs(input: Run.Result): Processor.OutputList = {
    Processor.Outputs(Seq("GenePredictions/Regions"))
  }

  /** For each phenotype output, process all the datasets for it.
    */
  override def processOutputs(outputs: Seq[String]): IO[Unit] = {
    val script = aws.uriOf("resources/pipeline/genepredictions/genePredictions.py")

    // cluster configuration used to process each phenotype
    val cluster = Cluster(
      name = name.toString,
      instances = 4,
      masterInstanceType = InstanceType.m5_2xlarge,
      slaveInstanceType = InstanceType.m5_2xlarge,
      configurations = Seq(
        ApplicationConfig.sparkEnv.withConfig(ClassificationProperties.sparkUsePython3)
      )
    )

    // create the single job
    val step = JobStep.PySpark(script)

    for {
      job <- aws.runJob(cluster, step)
      _   <- aws.waitForJob(job)
    } yield ()
  }
}
