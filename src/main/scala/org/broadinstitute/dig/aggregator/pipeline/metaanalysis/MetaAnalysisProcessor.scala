package org.broadinstitute.dig.aggregator.pipeline.metaanalysis

import cats._
import cats.effect._
import cats.implicits._

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aggregator.core.config.BaseConfig
import org.broadinstitute.dig.aggregator.core.emr._
import org.broadinstitute.dig.aggregator.core.processors._

/**
 * After all the variants for a particular phenotype have been processed and
 * partitioned, meta-analysis is run on them.
 *
 * This process runs METAL on the common variants for each ancestry (grouped
 * by dataset), then merges the rare variants across all ancestries, keeping
 * only the variants with the largest N (sample size) among them and writing
 * those back out.
 *
 * Next, trans-ethnic analysis (METAL) is run across all the ancestries, and
 * the output of that is written back to HDFS.
 *
 * The output of the ancestry-specific analysis is written to:
 *
 *  s3://dig-analysis-data/out/metaanalysis/ancestry-specific/<phenotype>/ancestry=?
 *
 * The output of the trans-ethnic analysis is written to:
 *
 *  s3://dig-analysis-data/out/metaanalysis/trans-ethnic/<phenotype>
 *
 * The inputs and outputs for this processor are expected to be phenotypes.
 */
class MetaAnalysisProcessor(name: Processor.Name, config: BaseConfig) extends RunProcessor(name, config) {

  /**
   * All the processors this processor depends on.
   */
  override val dependencies: Seq[Processor.Name] = Seq(
    MetaAnalysisPipeline.variantPartitionProcessor,
  )

  /**
   * All the job scripts that need to be uploaded to AWS.
   */
  override val resources: Seq[String] = Seq(
    "pipeline/metaanalysis/cluster-bootstrap.sh",
    "pipeline/metaanalysis/runAnalysis.py",
  )

  /**
   * Take all the phenotype results from the dependencies and process them.
   */
  override def processResults(results: Seq[Run.Result]): IO[Unit] = {
    val bootstrapScript = aws.uriOf("resources/pipeline/metaanalysis/cluster-bootstrap.sh")
    val script          = aws.uriOf("resources/pipeline/metaanalysis/runAnalysis.py")

    // collect unique phenotypes across all results to process
    val phenotypes = results.map(_.output).distinct

    // create a set of jobs for each phenotype
    val runs = for (phenotype <- phenotypes) yield {
      val cluster = Cluster(
        name = name.toString,
        bootstrapScripts = Seq(bootstrapScript),
        configurations = Seq(
          ApplicationConfig.sparkDefaults.withProperties("spark.network.timeout" -> "3600"),
        ),
      )

      // first run ancestry-specific and then trans-ethnic
      val steps = Seq(
        JobStep.PySpark(script, "--ancestry-specific", phenotype),
        JobStep.PySpark(script, "--trans-ethnic", phenotype),
      )

      for {
        _      <- IO(logger.info(s"Processing phenotype $phenotype..."))
        result <- aws.runJob(cluster, steps)
        _      <- Run.insert(xa, name, Seq(phenotype), phenotype)
      } yield result
    }

    // process each phenotype (could be done in parallel!)
    aws.waitForJobs(runs)
  }
}
