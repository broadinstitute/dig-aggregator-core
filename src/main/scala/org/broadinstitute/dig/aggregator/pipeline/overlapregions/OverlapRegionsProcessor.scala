package org.broadinstitute.dig.aggregator.pipeline.overlapregions

import cats.effect.IO
import org.broadinstitute.dig.aggregator.core.{Processor, Run}
import org.broadinstitute.dig.aggregator.core.config.BaseConfig
import org.broadinstitute.dig.aggregator.pipeline.gregor.GregorPipeline
import org.broadinstitute.dig.aggregator.pipeline.metaanalysis.MetaAnalysisPipeline
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._

class OverlapRegionsProcessor(name: Processor.Name, config: BaseConfig) extends Processor(name, config) {

  /** Dependency processors.
    */
  override val dependencies: Seq[Processor.Name] = Seq(
    GregorPipeline.sortRegionsProcessor,
    MetaAnalysisPipeline.metaAnalysisProcessor,
  )

  /** All the job scripts that need to be uploaded to AWS.
    */
  override val resources: Seq[String] = Seq(
    "pipeline/overlappedregions/overlapRegions.py",
  )

  /** All the regions are processed into a single output.
    */
  override def getOutputs(input: Run.Result): Processor.OutputList = {
    Processor.Outputs(Seq("overlapped-regions"))
  }

  /** With a new variants list or new regions, need to reprocess and get a list
    * of all regions with the variants that they overlap.
    */
  override def processOutputs(outputs: Seq[String]): IO[Unit] = {
    val script = aws.uriOf("resources/pipeline/overlapregions/overlapRegions.py")

    // cluster configuration used to process each phenotype
    val cluster = Cluster(
      name = name.toString,
      configurations = Seq(
        ApplicationConfig.sparkEnv.withConfig(ClassificationProperties.sparkUsePython3)
      )
    )

    // chromosomes to map
    val chrs = (1 to 22).map(_.toString) ++ Seq("X", "Y", "XY", "M")

    // create a job for variants and regions per chromosome
    val jobs = for (chr <- chrs; join <- Seq("--variants", "--regions")) yield {
      Seq(JobStep.PySpark(script, join, chr))
    }

    // cluster the jobs across multiple machines
    val clusteredJobs = aws.clusterJobs(cluster, jobs)

    // wait for all the jobs to complete
    aws.waitForJobs(clusteredJobs)
  }
}
