package org.broadinstitute.dig.aggregator.pipeline.frequencyanalysis

import org.broadinstitute.dig.aggregator.core.Processor
import org.broadinstitute.dig.aggregator.core.Run
import org.broadinstitute.dig.aggregator.core.config.BaseConfig
import org.broadinstitute.dig.aggregator.pipeline.intake.IntakePipeline
import org.broadinstitute.dig.aws.JobStep
import org.broadinstitute.dig.aws.emr.{Cluster, InstanceType, Spark}
import cats.effect.IO
import org.broadinstitute.dig.aggregator.core.DbPool

/** After all the variants for a particular phenotype have been uploaded, the
  * frequency processor runs a Spark job that will calculate the average EAF
  * and MAF for each variant across all datasets - partitioned by ancestry -
  * for which frequency data exists.
  *
  * Input HDFS data:
  *
  *  s3://dig-analysis-data/variants/<*>/<*>/part-*
  *
  * Output HDFS results:
  *
  *  s3://dig-analysis-data/out/frequencyanalysis/<ancestry>/part-*
  */
class FrequencyAnalysisProcessor(name: Processor.Name, config: BaseConfig, pool: DbPool)
    extends Processor(name, config, pool) {

  /** Source data to consume.
    */
  override val dependencies: Seq[Processor.Name] = Seq(IntakePipeline.variants)

  /** All the job scripts that need to be uploaded to AWS.
    */
  override val resources: Seq[String] = Seq(
    "pipeline/frequencyanalysis/frequencyAnalysis.py"
  )

  /* Cluster configuration used to process frequency.
   */
  override val cluster: Cluster = Cluster(
    name = name.toString,
    instances = 5,
    masterInstanceType = InstanceType.c5_9xlarge,
    slaveInstanceType = InstanceType.c5_9xlarge,
    masterVolumeSizeInGB = 500,
    slaveVolumeSizeInGB = 500,
    configurations = Seq(
      Spark.Env().withPython3,
      Spark.Config().withMaximizeResourceAllocation,
    )
  )

  /** Unique ancestries to process.
    */
  private val ancestries = Seq("AA", "AF", "EA", "EU", "HS", "SA")

  /** Each ancestry gets its own output.
    */
  override def getOutputs(input: Run.Result): Processor.OutputList = {
    Processor.Outputs(ancestries)
  }

  /** For each phenotype output, process all the datasets for it.
    */
  override def getJob(output: String): Seq[JobStep] = {
    val script   = aws.uriOf("resources/pipeline/frequencyanalysis/frequencyAnalysis.py")
    val ancestry = output

    // output is an ancestry
    Seq(JobStep.PySpark(script, ancestry))
  }
}
