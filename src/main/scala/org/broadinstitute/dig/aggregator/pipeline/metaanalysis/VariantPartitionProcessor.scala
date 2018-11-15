package org.broadinstitute.dig.aggregator.pipeline.metaanalysis

import cats._
import cats.effect._
import cats.implicits._

import com.amazonaws.services.elasticmapreduce.model.RunJobFlowResult

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aggregator.core.config.BaseConfig
import org.broadinstitute.dig.aggregator.core.emr.Cluster
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
    "pipeline/metaanalysis/partitionVariants.py",
  )

  /**
   * Take all the datasets that need to be processed, determine the phenotype
   * for each, and create a mapping of (phenotype -> datasets).
   */
  override def processDatasets(datasets: Seq[Dataset]): IO[Unit] = {
    val pattern = raw"([^/]+)/(.*)".r

    // get a list of all the dataset/phenotype pairs
    val datasetPhenotypes = datasets.map(_.dataset).distinct.collect {
      case pattern(dataset, phenotype) => (dataset, phenotype)
    }

    // create all the job to process each one
    val jobs = datasetPhenotypes.map {
      case (dataset, phenotype) => processDataset(dataset, phenotype)
    }

    // get a mapping of all the datasets (inputs) for each phenotype (output)
    val outputs = datasetPhenotypes.groupBy(_._2).mapValues { list =>
      list.map {
        case (dataset, phenotype) => s"$dataset/$phenotype"
      }
    }

    // create a list of all the results to write to the database
    val runs = outputs.toList.map {
      case (output, inputs) => Run.insert(pool, name, inputs, output)
    }

    // run all the jobs (note: this could be done in parallel!)
    for {
      _ <- aws.waitForJobs(jobs)
      _ <- runs.sequence
      _ <- IO(logger.info("Done"))
    } yield ()
  }

  /**
   * Spin up a cluster to process a single dataset.
   */
  private def processDataset(dataset: String, phenotype: String): IO[RunJobFlowResult] = {
    val script  = aws.uriOf("resources/pipeline/metaanalysis/partitionVariants.py")
    val cluster = Cluster(name = s"${name.toString} - $dataset/$phenotype")
    val step    = JobStep.PySpark(script, dataset, phenotype)

    for {
      _   <- IO(logger.info(s"Partitioning $dataset/$phenotype..."))
      job <- aws.runJob(cluster, step)
    } yield job
  }
}
