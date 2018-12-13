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
class VariantEffectProcessor(name: Processor.Name, config: BaseConfig) extends DatasetProcessor(name, config) {

  /**
   * Topic to consume.
   */
  override val topic: String = "variants"

  /**
   * All the job scripts that need to be uploaded to AWS.
   */
  override val resources: Seq[String] = Seq(
    "pipeline/varianteffect/cluster-bootstrap.sh",
    "pipeline/varianteffect/master-bootstrap.sh",
    "pipeline/varianteffect/prepareDatasets.py",
    "pipeline/varianteffect/runVEP.py",
    "pipeline/varianteffect/loadVariantEffects.py",
  )

  /**
   * All that matters is that there are new datasets. The input datasets are
   * actually ignored, and _everything_ is reprocessed. This is done because
   * there is only a single analysis node for all variants.
   */
  override def processDatasets(datasets: Seq[Dataset]): IO[Unit] = {
    val clusterBootstrap = aws.uriOf("resources/pipeline/varianteffect/cluster-bootstrap.sh")
    val masterBootstrap  = aws.uriOf("resources/pipeline/varianteffect/master-bootstrap.sh")
    val prepareScript    = aws.uriOf("resources/pipeline/varianteffect/prepareDatasets.py")
    val runScript        = aws.uriOf("resources/pipeline/varianteffect/runVEP.py")
    val loadScript       = aws.uriOf("resources/pipeline/varianteffect/loadVariantEffects.py")

    // build the cluster definition
    val cluster = Cluster(
      name = name.toString,
      masterInstanceType = InstanceType.c5_4xlarge,
      instances = 3,
      bootstrapScripts = Seq(
        new BootstrapScript(clusterBootstrap),
        new BootstrapScript(masterBootstrap) // TODO: Use MasterBootstrapScript once AWS fixes their bug!
      ),
    )

    // first prepare, then run VEP, finally, load VEP to S3
    val steps = Seq(
      //JobStep.PySpark(prepareScript),
      JobStep.Script(runScript),
      JobStep.PySpark(loadScript),
    )

    // run all the jobs (note: this could be done in parallel!)
    for {
      _   <- IO(logger.info(s"Running VEP..."))
      job <- aws.runJob(cluster, steps)
      _   <- aws.waitForJob(job)
      _   <- Run.insert(pool, name, datasets.map(_.dataset), "out/varianteffect/effects")
      _   <- IO(logger.info("Done"))
    } yield ()
  }
}
