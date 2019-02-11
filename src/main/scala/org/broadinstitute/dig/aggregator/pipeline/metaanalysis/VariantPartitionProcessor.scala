package org.broadinstitute.dig.aggregator.pipeline.metaanalysis

import cats._
import cats.effect._
import cats.implicits._

import com.amazonaws.services.elasticmapreduce.model.RunJobFlowResult

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aggregator.core.config.BaseConfig
import org.broadinstitute.dig.aggregator.core.emr.{Cluster, InstanceType}
import org.broadinstitute.dig.aggregator.core.processors._

/**
 * When a variants dataset has finished uploading, this processor takes the
 * dataset and partitions the variants for use in the meta-analysis processor
 * creating CSV data with the appropriate fields and partitioning them by
 * rarity and ancestry:
 *
 *  s3://dig-analysis-data/out/metaanalysis/variants/<phenotype>/<dataset>/common/ancestry=?
 *  s3://dig-analysis-data/out/metaanalysis/variants/<phenotype>/<dataset>/rare/ancestry=?
 */
class VariantPartitionProcessor(name: Processor.Name, config: BaseConfig) extends DatasetProcessor(name, config) {

  /**
   * Topic to consume.
   */
  override val topic: String = "variants"

  /**
   * All the job scripts that need to be uploaded to AWS.
   */
  override val resources: Seq[String] = Seq(
    "pipeline/metaanalysis/partitionVariants.py"
  )

  /**
   * Take all the datasets that need to be processed, determine the phenotype
   * for each, and create a mapping of (phenotype -> datasets).
   */
  override def processDatasets(datasets: Seq[Dataset]): IO[Unit] = {
    val pattern = raw"([^/]+)/(.*)".r

    // get a list of all the phenotypes that need processed
    val datasetPhenotypes = datasets.map(_.dataset).collect {
      case dataset @ pattern(_, phenotype) => (dataset, phenotype)
    }

    // map each phenotype to a list of datasets
    val phenotypeDatasets = datasetPhenotypes
      .groupBy(_._2)
      .mapValues(_.map(_._1).distinct)
      .toList

    // create the jobs to process each phenotype in parallel
    val jobs = phenotypeDatasets.map {
      case (phenotype, datasets) => processPhenotype(phenotype, datasets)
    }

    // create the runs for each phenotype
    val runs = phenotypeDatasets.map {
      case (phenotype, datasets) => Run.insert(pool, name, datasets, phenotype)
    }

    // run all the jobs then update the database
    for {
      _ <- aws.waitForJobs(jobs)
      _ <- IO(logger.info("Updating database..."))
      _ <- runs.sequence
      _ <- IO(logger.info("Done"))
    } yield ()
  }

  /**
   * Spin up a cluster to process a single phenotype.
   */
  private def processPhenotype(phenotype: String, datasets: Seq[String]): IO[RunJobFlowResult] = {
    val script = aws.uriOf("resources/pipeline/metaanalysis/partitionVariants.py")
    val step   = JobStep.PySpark(script, phenotype)
    val cluster = Cluster(
      name = name.toString,
      instances = 4,
      masterInstanceType = InstanceType.m5_2xlarge,
      slaveInstanceType = InstanceType.m5_2xlarge
    )

    for {
      _   <- IO(logger.info(s"Partitioning $phenotype datasets..."))
      job <- aws.runJob(cluster, step)
    } yield job
  }
}
