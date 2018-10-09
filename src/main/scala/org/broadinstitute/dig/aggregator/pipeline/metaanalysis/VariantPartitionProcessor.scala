package org.broadinstitute.dig.aggregator.pipeline.metaanalysis

import cats._
import cats.effect._
import cats.implicits._

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aggregator.core.config.BaseConfig
import org.broadinstitute.dig.aggregator.core.processors._

/**
 * When a variants dataset has finished uploading, this processor takes the
 * dataset and transforms it, ready for use by the rest of the pipeline. It
 * ensures that each variant...
 *
 *  * has a `varId`;
 *  * has a `beta` column (optionally derived from `oddRatio`);
 *  * has a `sampleSize` taken from the dataset's metadata;
 *
 * Once done, the variants are partitioned by phenotype, dataset, ancestry,
 * and then rarity:
 *
 *  file:///mnt/efs/metaanalysis/<phenotype>/<dataset>/common/ancestry=?/part-*
 *  file:///mnt/efs/metaanalysis/<phenotype>/<dataset>/rare/ancestry=?/part-*
 */
class VariantPartitionProcessor(config: BaseConfig) extends DatasetProcessor(config) {

  /**
   * Unique name identifying this processor.
   */
  val name: Processor.Name = Processors.variantPartitionProcessor

  /**
   * Topic to consume.
   */
  val topic: String = "variants"

  /**
   * All the job scripts that need to be uploaded to AWS.
   */
  val resources: Seq[String] = Seq(
    "pipeline/metaanalysis/partitionVariants.py",
  )

  /**
   * Take all the datasets that need to be processed, determine the phenotype
   * for each, and create a mapping of (phenotype -> datasets).
   */
  def processCommits(commits: Seq[Commit]): IO[Unit] = {
    val pattern = raw"([^/]+)/(.*)".r

    // extract the root and the phenotype from each "root/phenotype" dataset
    val datasets = commits.map(_.dataset).distinct.collect {
      case dataset @ pattern(_, phenotype) => (dataset, phenotype)
    }

    // create a map of the unique phenotypes and the roots mapping to them
    val phenotypes = datasets.map(_._2).distinct

    // process each phenotype as a separate "run"
    val phenotypeJobs = for (phenotype <- phenotypes) yield {
      processPhenotype(phenotype, datasets.filter(_._2 == phenotype).map(_._1))
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
    val script = resourceURI("pipeline/metaanalysis/partitionVariants.py")

    // create a job for each dataset
    val jobs = datasets.map { dataset =>
      val step = JobStep.PySpark(script, dataset)

      for {
        _ <- IO(logger.info(s"...$dataset"))
        _ <- aws.runStepAndWait(step)
      } yield ()
    }

    // run all the jobs (note: this could be done in parallel!)
    for {
      _ <- IO(logger.info(s"Processing $phenotype datasets..."))
      _ <- jobs.toList.sequence
      _ <- IO(logger.info("Done"))
      _ <- Run.insert(xa, name, datasets, phenotype)
    } yield ()
  }
}
