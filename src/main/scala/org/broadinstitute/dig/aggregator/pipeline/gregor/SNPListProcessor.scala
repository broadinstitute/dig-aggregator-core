package org.broadinstitute.dig.aggregator.pipeline.gregor

import cats.effect._

import java.util.UUID

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aggregator.core.config.BaseConfig
import org.broadinstitute.dig.aggregator.core.emr._
import org.broadinstitute.dig.aggregator.pipeline.metaanalysis._

/** Gathers all the output variants from the trans-ethnic, meta-analysis results
  * and generates a unique list of SNPs for use with GREGOR.
  *
  * Inputs:
  *
  *   s3://dig-analysis-data/out/metaanalysis/ancestry-specific
  *
  * Outputs:
  *
  *   s3://dig-analysis-data/out/gregor/snp/<phenotype>/<ancestry>
  */
class SNPListProcessor(name: Processor.Name, config: BaseConfig) extends Processor(name, config) {

  /** All the processors this processor depends on.
    */
  override val dependencies: Seq[Processor.Name] = Seq(
    MetaAnalysisPipeline.metaAnalysisProcessor
  )

  /** All the job scripts that need to be uploaded to AWS.
    */
  override val resources: Seq[String] = Seq(
    "pipeline/gregor/snplist.py"
  )

  /** Every phenotype processed gets its own output.
    */
  override def getRunOutputs(work: Seq[Run.Result]): Map[String, Seq[UUID]] = {
    val phenotypes = work.map(_.output).distinct

    phenotypes.map { phenotype =>
      phenotype -> work.filter(_.output == phenotype).map(_.uuid).distinct
    }.toMap
  }

  /** Find all the unique SNPs from all the output of the meta-analysis processor.
    */
  override def processResults(results: Seq[Run.Result]): IO[Unit] = {
    val script = aws.uriOf("resources/pipeline/gregor/snplist.py")

    // cluster configuration used to process each phenotype
    val cluster = Cluster(
      name = name.toString,
      masterInstanceType = InstanceType.m5_2xlarge,
      slaveInstanceType = InstanceType.m5_2xlarge,
      configurations = Seq(
        ApplicationConfig.sparkEnv.withConfig(ClassificationProperties.sparkUsePython3)
      )
    )

    // each phenotype gets its own snp list output
    val phenotypes = results.map(_.output).distinct

    // create a job per phenotype
    val jobs = phenotypes.map { phenotype =>
      Seq(JobStep.PySpark(script, phenotype))
    }

    // cluster the jobs
    val clusteredJobs = aws.clusterJobs(cluster, jobs)

    aws.waitForJobs(clusteredJobs)
  }
}
