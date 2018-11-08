package org.broadinstitute.dig.aggregator.pipeline.metaanalysis

import cats._
import cats.effect._
import cats.implicits._

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

    // extract the root and the phenotype from each "root/phenotype" dataset
    val datasetPhenotypes = datasets.map(_.dataset).distinct.collect {
      case pattern(root, phenotype) => (root, phenotype)
    }

    // create a map of the unique phenotypes and the roots mapping to them
    val phenotypes = datasetPhenotypes.map(_._2).distinct

    // process each phenotype as a separate "run"
    val phenotypeJobs = for (phenotype <- phenotypes) yield {
      processPhenotype(phenotype, datasetPhenotypes.filter(_._2 == phenotype).map(_._1))
    }

    // process each phenotype (this could be done in parallel!)
    phenotypeJobs.toList.sequence >> IO.unit
  }

  /**
   * Process all the datasets for a given phenotype. This ensures that the
   * output for downstream processors is the phenotype and that it is
   * completely ready to be processed once done.
   */
  def processPhenotype(phenotype: String, datasets: Seq[String]): IO[Unit] = {
    import Implicits.contextShift

    val script = aws.uriOf("resources/pipeline/metaanalysis/partitionVariants.py")

    // create a job for each dataset
    val jobs = datasets.map { dataset =>
      val cluster = Cluster(name = name.toString)
      val step    = JobStep.PySpark(script, dataset, phenotype)

      for {
        _  <- IO(logger.info(s"...$dataset/$phenotype"))
        io <- aws.runStep(cluster, step)
      } yield io
    }

    // create a unique list of dataset/phenotype pairs as inputs
    val inputs = datasets.map { dataset =>
      s"$dataset/$phenotype"
    }

    // run all the jobs (note: this could be done in parallel!)
    for {
      _ <- IO(logger.info(s"Processing $phenotype datasets..."))
      _ <- aws.runJobs(jobs)
      _ <- IO(logger.info("Done"))
      _ <- Run.insert(xa, name, inputs, phenotype)
    } yield ()
  }
}
