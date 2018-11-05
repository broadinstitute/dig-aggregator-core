package org.broadinstitute.dig.aggregator.pipeline.metaanalysis

import cats._
import cats.effect._
import cats.implicits._

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aggregator.core.config.BaseConfig
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
 *  s3://dig-analysis-data/out/metaanalysis/<phenotype>/ancestry-specific/<ancestry>
 *
 * The output of the trans-ethnic analysis is written to:
 *
 *  s3://dig-analysis-data/out/metaanalysis/<phenotype>/trans-ethnic
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
    "/pipeline/metaanalysis/runAnalysis.py",
  )

  /**
   * Take all the phenotype results from the dependencies and process them.
   */
  override def processResults(results: Seq[Run.Result]): IO[Unit] = {
    val script = aws.uriOf("resources/pipeline/metaanalysis/runAnalysis.py")

    // collect unique phenotypes across all results to process
    val phenotypes = results.map(_.output).distinct

    // create a set of jobs for each phenotype
    val runs = for (phenotype <- phenotypes) yield {
      val steps = Seq(
        JobStep.PySpark(script, "--ancestry-specific", phenotype),
        JobStep.PySpark(script, "--trans-ethnic", phenotype),
      )

      for {
        _ <- IO(logger.info(s"Processing phenotype $phenotype..."))

        // first run ancestry-specific analysis, followed by trans-ethnic
        _ <- aws.runJobAndWait(steps)

        // add the result to the database
        _ <- Run.insert(xa, name, Seq(phenotype), phenotype)
      } yield logger.info("Done")
    }

    // process each phenotype (could be done in parallel!)
    runs.toList.sequence >> IO.unit
  }
}
