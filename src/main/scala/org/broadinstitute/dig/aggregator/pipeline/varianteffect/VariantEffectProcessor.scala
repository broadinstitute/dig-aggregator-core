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
 * When a dataset has finished having all its variants listed in the proper
 * format for VEP, this processor takes those output part files and runs VEP
 * over them in parallel, uploading the results (as JSON list files) back
 * to HDFS.
 *
 * VEP TSV input files located at:
 *
 *  s3://dig-analysis-data/out/varianteffect/variants
 *
 * VEP output JSON written to:
 *
 *  s3://dig-analysis-data/out/varianteffect/effects
 */
class VariantEffectProcessor(name: Processor.Name, config: BaseConfig) extends RunProcessor(name, config) {

  /**
   * All the processors this processor depends on.
   */
  override val dependencies: Seq[Processor.Name] = Seq(
    VariantEffectPipeline.variantListProcessor
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
    val clusterBootstrap = aws.uriOf("resources/pipeline/varianteffect/cluster-bootstrap.sh")
    val masterBootstrap  = aws.uriOf("resources/pipeline/varianteffect/master-bootstrap.sh")

    // get a list of distinct datasets that need VEP run on them
    val datasets = results.map(_.output).distinct
    val jobs     = datasets.map(processDataset)

    // each cluster is a single, beefy machine that only runs VEP
    val cluster = Cluster(
      name = name.toString,
      masterInstanceType = InstanceType.c5_4xlarge,
      instances = 1,
      bootstrapScripts = Seq(
        new BootstrapScript(clusterBootstrap),
        new BootstrapScript(masterBootstrap)
      )
    )

    for {
      jobs <- datasets.map(processDataset).toList.sequence

      /*
       * As there's nothing special about the part files per dataset, it's OK
       * if the parts are done in random order. All the part files across
       * the datasets should be flattened so that they are fairly evenly
       * distributed among the clusters. At this point, every "job" is a single
       * step.
       */
      flattened = jobs.flatten.map(Seq.apply(_))

      // distribute the jobs across many clustered machines
      clusteredJobs = aws.clusterJobs(cluster, flattened, maxClusters = 20)

      // wait for the work to finish
      _ <- IO(logger.info("Running VEP..."))
      _ <- aws.waitForJobs(clusteredJobs)
      _ <- IO(logger.info("Updating database..."))
      _ <- Run.insert(pool, name, datasets, "VEP")
      _ <- IO(logger.info("Done"))
    } yield ()
  }

  /**
   * This function finds all the part files in AWS for a given dataset, then
   * breaks them up into groups and creates a cluster to process each group.
   */
  private def processDataset(dataset: String): IO[Seq[JobStep]] = {
    val runScript = aws.uriOf("resources/pipeline/varianteffect/runVEP.py")

    for {
      // delete all the existing effects for the dataset
      _ <- aws.rmdir(s"out/varianteffect/effects/$dataset/")

      // get all the variant part files to process
      keys <- aws.ls(s"out/varianteffect/variants/$dataset/", excludeSuccess = true)

      // only use the filename, not the entire key
      parts = keys.map(_.split('/').last)

      // generate a step to run VEP for each part file
    } yield parts.map(part => JobStep.Script(runScript, dataset, part))
  }
}
