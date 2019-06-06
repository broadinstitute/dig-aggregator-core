package org.broadinstitute.dig.aggregator.pipeline.gregor

import cats.effect._

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aggregator.core.config.BaseConfig
import org.broadinstitute.dig.aggregator.core.emr._
import org.broadinstitute.dig.aggregator.core.processors._

class OverlapRegionsProcessor(name: Processor.Name, config: BaseConfig) extends RunProcessor(name, config) {

  /** Dependency processors.
    */
  override val dependencies: Seq[Processor.Name] = Seq(
    GregorPipeline.sortRegionsProcessor,
  )

  /** All the job scripts that need to be uploaded to AWS.
    */
  override val resources: Seq[String] = Seq(
    "pipeline/gregor/overlapRegions.py"
  )

  /** All datasets and VEP output map to a single output.
    */
  override def getRunOutputs(work: Seq[Run.Result]): Map[String, Seq[String]] = {
    Map("overlapped-regions" -> work.map(_.output).distinct)
  }

  /** With a new variants list or new regions, need to reprocess and get a list
    * of all regions with the variants that they overlap.
    */
  override def processResults(results: Seq[Run.Result]): IO[Unit] = {
    val script = aws.uriOf("resources/pipeline/gregor/overlapRegions.py")

    // cluster configuration used to process each phenotype
    val cluster = Cluster(
      name = name.toString,
      configurations = Seq(
        ApplicationConfig.sparkEnv.withConfig(ClassificationProperties.sparkUsePython3)
      )
    )

    // all the chromosomes in the genome (excludes XY and M for now)
    val chromosomes = (1 to 22).map(_.toString) ++ Seq("X", "Y", "XY", "M")

    // create a job per chromosome
    val jobs = chromosomes.map { chromosome =>
      Seq(JobStep.PySpark(script, chromosome))
    }

    // cluster the jobs across multiple machines
    val clusteredJobs = aws.clusterJobs(cluster, jobs)

    // run all the jobs
    aws.waitForJobs(clusteredJobs)
  }
}
