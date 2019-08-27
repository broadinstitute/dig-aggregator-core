package org.broadinstitute.dig.aggregator.pipeline.gregor

import org.broadinstitute.dig.aggregator.core.Processor
import org.broadinstitute.dig.aggregator.core.Run
import org.broadinstitute.dig.aggregator.core.config.BaseConfig
import org.broadinstitute.dig.aws.JobStep
import org.broadinstitute.dig.aws.emr.ApplicationConfig
import org.broadinstitute.dig.aws.emr.ClassificationProperties
import org.broadinstitute.dig.aws.emr.Cluster

import cats.effect.IO

class OverlapRegionsProcessor(name: Processor.Name, config: BaseConfig) extends Processor(name, config) {

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

  /** All the regions are processed into a single output.
    */
  override def getOutputs(input: Run.Result): Processor.OutputList = {
    Processor.Outputs(Seq("overlapped-regions"))
  }

  /** With a new variants list or new regions, need to reprocess and get a list
    * of all regions with the variants that they overlap.
    */
  override def processOutputs(outputs: Seq[String]): IO[Unit] = {
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
