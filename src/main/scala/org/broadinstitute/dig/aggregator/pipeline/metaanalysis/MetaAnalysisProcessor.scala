package org.broadinstitute.dig.aggregator.pipeline.metaanalysis

import cats._
import cats.effect._
import cats.implicits._

import com.amazonaws.services.elasticmapreduce.model.RunJobFlowResult

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
    MetaAnalysisPipeline.variantPartitionProcessor
  )

  /**
   * All the job scripts that need to be uploaded to AWS.
   */
  override val resources: Seq[String] = Seq(
    "pipeline/metaanalysis/cluster-bootstrap.sh",
    "pipeline/metaanalysis/runAnalysis.py",
    "pipeline/metaanalysis/loadAnalysis.py",
    "scripts/getmerge-strip-headers.sh"
  )

  /**
   * Take all the phenotype results from the dependencies and process them.
   */
  override def processResults(results: Seq[Run.Result]): IO[Unit] = {
    val phenotypes = results.map(_.output).distinct.toList

    // create a set of jobs to process the phenotypes
    val jobs = phenotypes.map(processPhenotype)

    // create the runs for each phenotype
    val runs = phenotypes.map { phenotype =>
      Run.insert(pool, name, Seq(phenotype), phenotype)
    }

    // wait for all the jobs to complete, then insert all the results
    for {
      _ <- aws.waitForJobs(jobs)
      _ <- IO(logger.info("Updating database..."))
      _ <- runs.sequence
      _ <- IO(logger.info("Done"))
    } yield ()
  }

  /**
   * Create a cluster and process a single phenotype.
   */
  private def processPhenotype(phenotype: String): IO[RunJobFlowResult] = {
    val bootstrapUri = aws.uriOf("resources/pipeline/metaanalysis/cluster-bootstrap.sh")
    val runUri       = aws.uriOf("resources/pipeline/metaanalysis/runAnalysis.py")
    val loadUri      = aws.uriOf("resources/pipeline/metaanalysis/loadAnalysis.py")

    val sparkConf = ApplicationConfig.sparkEnv.withProperties(
      "PYSPARK_PYTHON" -> "/usr/bin/python3"
    )

    // EMR cluster to run the job steps on
    val cluster = Cluster(
      name = name.toString,
      bootstrapScripts = Seq(new BootstrapScript(bootstrapUri)),
      configurations = Seq(sparkConf)
    )

    // first run+load ancestry-specific and then trans-ethnic
    val steps = Seq(
      JobStep.Script(runUri, "--ancestry-specific", phenotype),
      JobStep.PySpark(loadUri, "--ancestry-specific", phenotype),
      JobStep.Script(runUri, "--trans-ethnic", phenotype),
      JobStep.PySpark(loadUri, "--trans-ethnic", phenotype)
    )

    for {
      _   <- IO(logger.info(s"Processing phenotype $phenotype..."))
      job <- aws.runJob(cluster, steps)
    } yield job
  }
}
