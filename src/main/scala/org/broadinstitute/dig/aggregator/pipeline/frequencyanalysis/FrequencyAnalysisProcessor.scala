package org.broadinstitute.dig.aggregator.pipeline.frequencyanalysis

import cats._
import cats.effect._
import cats.implicits._

import com.amazonaws.services.elasticmapreduce.model.RunJobFlowResult

import java.util.UUID

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aggregator.core.config.BaseConfig
import org.broadinstitute.dig.aggregator.core.emr._
import org.broadinstitute.dig.aggregator.pipeline.intake.IntakePipeline

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
class FrequencyAnalysisProcessor(name: Processor.Name, config: BaseConfig) extends Processor(name, config) {

  /** Source data to consume.
    */
  override val dependencies: Seq[Processor.Name] = Seq(IntakePipeline.variants)

  /** All the job scripts that need to be uploaded to AWS.
    */
  override val resources: Seq[String] = Seq(
    "pipeline/frequencyanalysis/frequencyAnalysis.py"
  )

  /** Determine the output(s) for each input.
    *
    * The inputs are the dataset names. The outputs are the phenotype represented
    * within that dataset.
    */
  override def getOutputs(input: Run.Result): Processor.OutputList = {
    val pattern = raw"([^/]+)/(.*)".r

    input.output match {
      case pattern(_, phenotype) => Processor.Outputs(Seq(phenotype))
    }
  }

  /** For each phenotype output, process all the datasets for it.
    */
  override def processOutputs(outputs: Seq[String]): IO[Unit] = {
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
    val jobs = outputs.toList.map { phenotype =>
      Seq(JobStep.PySpark(script, phenotype))
    }

    // distribute the jobs among multiple clusters
    val clusteredJobs = aws.clusterJobs(cluster, jobs)

    // run all the jobs then update the database
    aws.waitForJobs(clusteredJobs)
  }
}
