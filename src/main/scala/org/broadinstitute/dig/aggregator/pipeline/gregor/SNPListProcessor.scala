package org.broadinstitute.dig.aggregator.pipeline.gregor

import cats.effect._

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aggregator.core.config.BaseConfig
import org.broadinstitute.dig.aggregator.core.emr._
import org.broadinstitute.dig.aggregator.core.processors._
import org.broadinstitute.dig.aggregator.pipeline.metaanalysis._

/** Gathers all the output variants from the trans-ethnic, meta-analysis results
  * and generates a unique list of SNPs for use with GREGOR.
  *
  * Inputs:
  *
  *   s3://dig-analysis-data/out/metaanalysis/trans-ethnic
  *
  * Outputs:
  *
  *   s3://dig-analysis-data/out/gregor/snp
  */
class SNPListProcessor(name: Processor.Name, config: BaseConfig) extends RunProcessor(name, config) {

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

  /** Find all the unique SNPs from all the output of the meta-analysis processor.
    */
  def processResults(results: Seq[Run.Result]): IO[Unit] = {
    val script = aws.uriOf("resources/pipeline/gregor/snplist.py")

    // cluster configuration used to process each phenotype
    val cluster = Cluster(
      name = name.toString,
      masterInstanceType = InstanceType.m5_2xlarge,
      slaveInstanceType = InstanceType.m5_2xlarge,
      configurations = Seq(
        ApplicationConfig.sparkEnv.withProperties(
          "PYSPARK_PYTHON" -> "/usr/bin/python3"
        )
      )
    )

    // create the jobs to process each phenotype in parallel
    val step = JobStep.PySpark(script)
    val run  = Run.insert(pool, name, results.map(_.output), "SNPList")

    for {
      job <- aws.runJob(cluster, step)
      _   <- aws.waitForJob(job)
      _   <- IO(logger.info("Updating database..."))
      _   <- run
      _   <- IO(logger.info("Done"))
    } yield ()
  }
}
