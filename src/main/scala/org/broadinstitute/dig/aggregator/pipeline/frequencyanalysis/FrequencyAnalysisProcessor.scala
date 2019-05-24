package org.broadinstitute.dig.aggregator.pipeline.frequencyanalysis

import cats._
import cats.effect._
import cats.implicits._

import com.amazonaws.services.elasticmapreduce.model.RunJobFlowResult

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aggregator.core.config.BaseConfig
import org.broadinstitute.dig.aggregator.core.emr._
import org.broadinstitute.dig.aggregator.core.processors._

/** After all the variants for a particular phenotype have been uploaded, the
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

  /** Topic to consume.
    */
  override val topic: String = "variants"

  /** All the job scripts that need to be uploaded to AWS.
    */
  override val resources: Seq[String] = Seq(
    "pipeline/frequencyanalysis/frequencyAnalysis.py"
  )

  /** Parse datasets to extract a phenotype -> [Dataset] mapping.
    */
  override def getRunOutputs(datasets: Seq[Dataset]): Map[String, Seq[String]] = {
    val pattern = raw"([^/]+)/(.*)".r

    val phenotypeMapping = datasets.map(_.dataset).map {
      case dataset @ pattern(_, phenotype) => (phenotype, dataset)
    }

    phenotypeMapping.groupBy(_._1).mapValues(_.map(_._2))
  }

  /** Take all the datasets that need to be processed, determine the phenotype
    * for each, and create a mapping of (phenotype -> datasets).
    */
  override def processDatasets(datasets: Seq[Dataset]): IO[Unit] = {
    val script = aws.uriOf("resources/pipeline/frequencyanalysis/frequencyAnalysis.py")

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

    // create the jobs to process each phenotype in parallel
    val jobs = getRunOutputs(datasets).keys.map { phenotype =>
      Seq(JobStep.PySpark(script, phenotype))
    }

    // distribute the jobs among multiple clusters
    val clusteredJobs = aws.clusterJobs(cluster, jobs.toSeq)

    // run all the jobs then update the database
    aws.waitForJobs(clusteredJobs)
  }
}
