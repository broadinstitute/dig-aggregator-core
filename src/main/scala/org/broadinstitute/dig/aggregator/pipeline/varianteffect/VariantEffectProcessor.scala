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
 *  s3://dig-analysis-data/out/varianteffect/<dataset>/variants
 *
 * VEP output files written to:
 *
 *  s3://dig-analysis-data/out/varianteffect/<dataset>/effects
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
    "pipeline/varianteffect/prepareDataset.py",
    "pipeline/varianteffect/runVEP.py",
  )

  /**
   * Take all the datasets that need to be processed, determine the phenotype
   * for each, and create a mapping of (phenotype -> datasets).
   */
  override def processDatasets(datasets: Seq[Dataset]): IO[Unit] = {
    val pattern = raw"([^/]+)/(.*)".r

    // get a unique list of all the dataset names
    val datasetNames = datasets.map(_.dataset).distinct

    // get the distinct list of study/phenotypes
    val studyPhenotypes = datasetNames.collect {
      case pattern(study, phenotype) => (study, phenotype)
    }

    // create all the job to process each one
    val jobs = studyPhenotypes.map {
      case (study, phenotype) => processDataset(study, phenotype)
    }

    // create a list of all the results to write to the database
    val runs = datasetNames.map { dataset =>
      Run.insert(pool, name, Seq(dataset), dataset)
    }

    // run all the jobs (note: this could be done in parallel!)
    for {
      _ <- aws.waitForJobs(jobs)
      _ <- runs.toList.sequence
      _ <- IO(logger.info("Done"))
    } yield ()
  }

  /**
   * Spin up a cluster to process a single study across all phenotypes.
   */
  private def processDataset(study: String, phenotype: String): IO[RunJobFlowResult] = {
    val clusterBootstrap = aws.uriOf("resources/pipeline/varianteffect/cluster-bootstrap.sh")
    val masterBootstrap  = aws.uriOf("resources/pipeline/varianteffect/master-bootstrap.sh")
    val prepareScript    = aws.uriOf("resources/pipeline/varianteffect/prepareDataset.py")
    val runScript        = aws.uriOf("resources/pipeline/varianteffect/runVEP.py")

    // build the cluster definition
    val cluster = Cluster(
      name = name.toString,
      bootstrapScripts = Seq(
        new BootstrapScript(clusterBootstrap),
        new BootstrapScript(masterBootstrap) // TODO: Use MasterBootstrapScript once AWS fixes their bug!
      ),
    )

    // first prepare, then run VEP, finally, load VEP to S3
    val steps = Seq(
      //JobStep.PySpark(prepareScript, study, phenotype),
      JobStep.Script(runScript, study, phenotype),
    )

    for {
      _   <- IO(logger.info(s"Running VEP on $study/$phenotype..."))
      job <- aws.runJob(cluster, steps)
    } yield job
  }
}
