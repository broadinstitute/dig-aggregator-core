package org.broadinstitute.dig.aggregator.pipeline.varianteffect

import cats._
import cats.effect._
import cats.implicits._

import com.amazonaws.services.elasticmapreduce.model.RunJobFlowResult

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aggregator.core.config.BaseConfig
import org.broadinstitute.dig.aggregator.core.emr._
import org.broadinstitute.dig.aggregator.core.processors._

/**
 * When a variants dataset has finished uploading, this processor takes the
 * dataset across all phenotypes and first prepares it for VEP and then runs
 * VEP on it, finally writing the output back to HDFS:
 *
 * VEP TSV files written to:
 *
 *  s3://dig-analysis-data/out/varianteffect/variants
 *
 * VEP output CSV files written to:
 *
 *  s3://dig-analysis-data/out/varianteffect/effects
 */
class VariantEffectProcessor(name: Processor.Name, config: BaseConfig) extends RunProcessor(name, config) {

  /**
   * All the processors this processor depends on.
   */
  override val dependencies: Seq[Processor.Name] = Seq(
    VariantEffectPipeline.variantEffectProcessor
  )

  /**
   * All the job scripts that need to be uploaded to AWS.
   */
  override val resources: Seq[String] = Seq(
    "pipeline/varianteffect/cluster-bootstrap.sh",
    "pipeline/varianteffect/master-bootstrap.sh",
    "pipeline/varianteffect/runVEP.py"
  )

  /**
   * Any datasets that have a new/updated list of variants needs to have VEP
   * run across them.
   */
  override def processResults(results: Seq[Run.Result]): IO[Unit] = {
    val datasets = results.map(_.output).distinct

    // create a job for each dataset
    val tasks = datasets.map(processDataset)

    // run all the jobs then update the database
    for {
      _ <- tasks.toList.sequence
      _ <- IO(logger.info("Done"))
    } yield ()
  }

  /**
   * This function finds all the part files in AWS for a given dataset, then
   * breaks them up into groups and creates a cluster to process each group.
   */
  private def processDataset(dataset: String): IO[Unit] = {
    val parallelClusters = 5

    for {
      _     <- aws.rmdir(s"out/varianteffect/effects/$dataset/")
      parts <- aws.ls(s"out/varianteffect/variants/$dataset/")

      // split the part files up into equal groups for each cluster
      groupSize = (parts.size / parallelClusters) + 1

      // if there are fewer parts than groups, make a VM per part
      groups = parts.grouped(groupSize).toSeq

      // create a job for each group
      jobs = groups.map(runVEP(dataset, _))

      // wait for all the jobs to complete
      _ <- aws.waitForJobs(jobs)

      // update the database with the completed output
      _ <- IO(logger.info("Updating database..."))
      _ <- Run.insert(pool, name, Seq(dataset), "VEP")
    } yield ()
  }

  /**
   * Every dataset is broken up into multiple part files of variants created
   * by the VariantListProcessor.
   *
   * This function takes a sequence of those part files, creates a cluster with
   * a single machine, and processes them. These parts will be processed in-
   * order (read: serially). It is intended that this function will be called
   * multiple times so that series of part files may be processed in parallel.
   *
   * For example, if there are 200 part files, they could be processed 10 at
   * a time using a window function:
   *
   *   allParts.grouped(10).toSeq.map(runVep(dataset, _))
   */
  private def runVEP(dataset: String, parts: Seq[String]): IO[RunJobFlowResult] = {
    val clusterBootstrap = aws.uriOf("resources/pipeline/varianteffect/cluster-bootstrap.sh")
    val masterBootstrap  = aws.uriOf("resources/pipeline/varianteffect/master-bootstrap.sh")
    val runScript        = aws.uriOf("resources/pipeline/varianteffect/runVEP.py")

    // build the cluster definition
    val cluster = Cluster(
      name = name.toString,
      masterInstanceType = InstanceType.c5_4xlarge,
      instances = 1,
      bootstrapScripts = Seq(
        new BootstrapScript(clusterBootstrap),
        new BootstrapScript(masterBootstrap)
      )
    )

    // create a step for every part file - they will be run in sequence
    val steps = parts.map { part =>
      JobStep.Script(runScript, dataset, part)
    }

    // run all the jobs (note: this could be done in parallel!)
    aws.runJob(cluster, steps)
  }
}
