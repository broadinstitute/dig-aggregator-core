package org.broadinstitute.dig.aggregator.pipeline.metaanalysis

import cats._
import cats.effect._
import cats.implicits._

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aggregator.core.config.BaseConfig
import org.broadinstitute.dig.aggregator.core.processors._

/**
 * After all the variants for a particular phenotype have been processed and
 * partitioned, the ancestry-specific analysis is run on them.
 *
 * This process runs METAL on the common variants for each ancestry (grouped
 * by dataset), then merges the rare variants across all ancestries, keeping
 * only the variants with the largest N (sample size) among them and writing
 * those back out for the next processor.
 *
 * Additionally, the output of METAL is also written to an S3 location so that
 * it can be uploaded to Neo4j to create :Frequency nodes:
 *
 *  s3://dig-analysis-data/out/metaanalysis/<phenotype>/ancestry-specific/<ancestry>
 *
 * The METAANALYSIS files for the trans-ethnic analysis are written to:
 *
 *  file:///mnt/efs/metaanalysis/<phenotype>/_analysis/ancestry-specific/<ancestry>/_combined
 *
 * The inputs for this processor are expected to be phenotypes.
 *
 * The outputs for this processor are phenotypes.
 */
class TransEthnicProcessor(name: Processor.Name, config: BaseConfig) extends RunProcessor(name, config) {

  /**
   * All the processors this processor depends on.
   */
  override val dependencies: Seq[Processor.Name] = Seq(
    MetaAnalysisPipeline.ancestrySpecificProcessor,
  )

  /**
   * All the job scripts that need to be uploaded to AWS.
   */
  override val resources: Seq[String] = Seq(
    "pipeline/metaanalysis/runAnalysis.py",
  )

  /**
   * Take all the phenotype results from the dependencies and process them.
   */
  override def processResults(results: Seq[Run.Result]): IO[Unit] = {
    val script     = aws.uriOf("resources/pipeline/metaanalysis/runAnalysis.py")
    val phenotypes = results.map(_.output).distinct

    // create runs for every phenotype
    val runs = for (phenotype <- phenotypes) yield {
      val step = JobStep.PySpark(script, "--trans-ethnic", phenotype)

      for {
        _ <- IO(logger.info(s"Processing phenotype $phenotype..."))
        _ <- aws.runStepAndWait(step)

        // the output location where the data was written to
        output = s"metaanalysis/$phenotype/_analysis/trans-ethnic"

        // add the results to the database
        _ <- Run.insert(xa, name, Seq(phenotype), phenotype)
        _ <- IO(logger.info("Done"))
      } yield ()
    }

    // process each phenotype (could be parallel!)
    runs.toList.sequence >> IO.unit
  }
}
