package org.broadinstitute.dig.aggregator.pipeline.frequencyanalysis

import cats._
import cats.effect._
import cats.implicits._

import com.amazonaws.services.elasticmapreduce.model.RunJobFlowResult

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aggregator.core.config.BaseConfig
import org.broadinstitute.dig.aggregator.core.emr._
import org.broadinstitute.dig.aggregator.core.processors._

/**
  * After all the variants for a particular phenotype have been uploaded, the
  * frequency processor runs a Spark job that will calculate the average EAF
  * and MAF for each variant across all datasets - partitioned by ancestry -
  * for which frequency data exists.
  *
  * Input HDFS data:
  *
  *  s3://dig-analysis-data/variants/<dataset>/<phenotype>/part-*
  *
  * Output HDFS results:
  *
  *  s3://dig-analysis-data/out/frequencyanalysis/<phenotype>/part-*
  */
class FrequencyAnalysisProcessor(name: Processor.Name, config: BaseConfig) extends DatasetProcessor(name, config) {

  /**
    * Topic to consume.
    */
  override val topic: String = "variants"

  /**
    * All the job scripts that need to be uploaded to AWS.
    */
  override val resources: Seq[String] = Seq(
    "pipeline/frequencyanalysis/frequencyAnalysis.py"
  )

  /**
    * Take all the datasets that need to be processed, determine the phenotype
    * for each, and create a mapping of (phenotype -> datasets).
    */
  override def processDatasets(datasets: Seq[Dataset]): IO[Unit] = {
    val script  = aws.uriOf("resources/pipeline/frequencyanalysis/frequencyAnalysis.py")
    val pattern = raw"([^/]+)/(.*)".r

    // cluster configuration used to process each phenotype
    val cluster = Cluster(
      name = name.toString,
      instances = 4,
      masterInstanceType = InstanceType.m5_2xlarge,
      slaveInstanceType = InstanceType.m5_2xlarge,
      configurations = Seq(
        ApplicationConfig.sparkEnv.withConfig(ClassificationProperties.sparkUsePython3)
      )
    )

    // get a list of all the phenotypes that need processed
    val datasetPhenotypes = datasets.map(_.dataset).collect {
      case dataset @ pattern(_, phenotype) => (phenotype, dataset)
    }

    // map each phenotype to a list of datasets
    val phenotypes = datasetPhenotypes.map(_._1).toList.distinct

    // create the jobs to process each phenotype in parallel
    val jobs = phenotypes.map { phenotype =>
      Seq(JobStep.PySpark(script, phenotype))
    }

    // distribute the jobs among multiple clusters
    val clusteredJobs = aws.clusterJobs(cluster, jobs)

    // create the runs for each phenotype
    val runs = phenotypes.map { phenotype =>
      val datasets = datasetPhenotypes.collect {
        case (p, dataset) if p == phenotype => dataset
      }

      // each phenotype's input is the list of datasets
      Run.insert(pool, name, datasets, phenotype)
    }

    // run all the jobs then update the database
    for {
      _ <- aws.waitForJobs(clusteredJobs)
      _ <- IO(logger.info("Updating database..."))
      _ <- runs.sequence
      _ <- IO(logger.info("Done"))
    } yield ()
  }
}
