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
 * Finds all the variants in a dataset across all phenotypes and writes them
 * out to a set of files that can have VEP run over in parallel.
 *
 * VEP input files written to:
 *
 *  s3://dig-analysis-data/out/varianteffect/variants/<dataset>
 */
class VariantListProcessor(name: Processor.Name, config: BaseConfig) extends DatasetProcessor(name, config) {

  /**
   * Topic to consume.
   */
  override val topic: String = "variants"

  /**
   * All the job scripts that need to be uploaded to AWS.
   */
  override val resources: Seq[String] = Seq(
    "pipeline/varianteffect/listVariants.py"
  )

  /**
   * Spark configuration for cluster.
   */
  private val sparkConf: ApplicationConfig = ApplicationConfig.sparkEnv.withProperties(
    "PYSPARK_PYTHON" -> "/usr/bin/python3"
  )

  /**
   * Definition for Cluster used to run this job.
   */
  private val cluster: Cluster = Cluster(
    name = name.toString,
    configurations = Seq(sparkConf)
  )

  /**
   * All that matters is that there are new datasets. The input datasets are
   * actually ignored, and _everything_ is reprocessed. This is done because
   * there is only a single analysis node for all variants.
   */
  override def processDatasets(datasets: Seq[Dataset]): IO[Unit] = {
    val pyScript = aws.uriOf("resources/pipeline/varianteffect/listVariants.py")
    val pattern  = raw"([^/]+)/(.*)".r

    // get a list of all the new and updated datasets
    val datasetInputs = datasets.map(_.dataset).collect {
      case input @ pattern(dataset, _) => (dataset, input)
    }

    // group by dataset and list all the new/updated phenotypes for each
    val uniqueDatasets = datasetInputs.groupBy(_._1).mapValues(_.map(_._2))

    // create a job (sequence of steps) for each dataset
    val jobs = uniqueDatasets.map {
      case (dataset, _) => Seq(JobStep.PySpark(pyScript, dataset))
    }

    // start all the jobs running across several clusters
    val clusteredJobs = aws.clusterJobs(cluster, jobs.toSeq)

    // map all the dataset tables as the inputs to the single output
    val runs = uniqueDatasets.map {
      case (dataset, inputs) => Run.insert(pool, name, inputs, dataset)
    }

    // run all the jobs then update the database
    for {
      _ <- IO(logger.info("Processing datasets..."))
      _ <- aws.waitForJobs(clusteredJobs)
      _ <- IO(logger.info("Updating database..."))
      _ <- runs.toList.sequence
      _ <- IO(logger.info("Done"))
    } yield ()
  }
}
