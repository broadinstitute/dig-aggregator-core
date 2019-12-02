package org.broadinstitute.dig.aggregator.pipeline.bioindex

import org.broadinstitute.dig.aggregator.core.Processor
import org.broadinstitute.dig.aggregator.core.Run
import org.broadinstitute.dig.aggregator.core.config.BaseConfig
import org.broadinstitute.dig.aggregator.pipeline.metaanalysis.MetaAnalysisPipeline
import org.broadinstitute.dig.aggregator.pipeline.gregor.GregorPipeline
import org.broadinstitute.dig.aws.JobStep
import org.broadinstitute.dig.aws.emr.ApplicationConfig
import org.broadinstitute.dig.aws.emr.ClassificationProperties
import org.broadinstitute.dig.aws.emr.Cluster
import org.broadinstitute.dig.aws.emr.InstanceType

import cats.effect.IO
import org.broadinstitute.dig.aggregator.core.DbPool

/** After running meta-analysis or gregor, the outputs are joined together with
  * other data, sorted by locus, and written to the bio index bucket so they
  * can be queried.
  */
class BioIndexProcessor(name: Processor.Name, config: BaseConfig, pool: DbPool) extends Processor(name, config, pool) {

  /** Source data to consume.
    */
  override val dependencies: Seq[Processor.Name] = Seq(
    MetaAnalysisPipeline.metaAnalysisProcessor,
    GregorPipeline.globalEnrichmentProcessor,
  )

  /** All the job scripts that need to be uploaded to AWS.
    */
  override val resources: Seq[String] = Seq(
    "pipeline/bioindex/variants.py",
    "pipeline/bioindex/regions.py",
  )

  /** Each ancestry gets its own output.
    */
  override def getOutputs(input: Run.Result): Processor.OutputList = {
    input.processor match {
      case MetaAnalysisPipeline.metaAnalysisProcessor => Processor.Outputs(Seq("variants"))
      case GregorPipeline.globalEnrichmentProcessor   => Processor.Outputs(Seq("regions"))
    }
  }

  /** For each phenotype output, process all the datasets for it.
    */
  override def processOutputs(outputs: Seq[String]): IO[Unit] = {
    val variantsScript = aws.uriOf("resources/pipeline/bioindex/variants.py")
    val regionsScript  = aws.uriOf("resources/pipeline/bioindex/regions.py")

    // cluster configuration used to process each phenotype
    val cluster = Cluster(
      name = name.toString,
      instances = 3,
      masterInstanceType = InstanceType.r5_2xlarge,
      slaveInstanceType = InstanceType.r5_2xlarge,
      masterVolumeSizeInGB = 500,
      slaveVolumeSizeInGB = 500,
      configurations = Seq(
        ApplicationConfig.sparkEnv.withConfig(ClassificationProperties.sparkUsePython3)
      )
    )

    // run the jobs
    val jobs = outputs.map {
      case "variants" => Seq(JobStep.PySpark(variantsScript))
      case "regions"  => Seq(JobStep.PySpark(regionsScript))
    }

    // distribute across clusters
    val clusteredJobs = aws.clusterJobs(cluster, jobs)

    aws.waitForJobs(clusteredJobs)
  }
}
