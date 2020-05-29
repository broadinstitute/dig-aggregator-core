package org.broadinstitute.dig.aggregator.pipeline.frequencyanalysis

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws.Ec2.Strategy
import org.broadinstitute.dig.aws.{JobStep, MemorySize}
import org.broadinstitute.dig.aws.emr.ClusterDef

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
class FrequencyAnalysisStage(implicit context: Context) extends Stage {
  import MemorySize.Implicits._

  var variants: Input.Source = Input.Source.Dataset("variants/")

  /** Input sources. */
  override val sources: Seq[Input.Source] = Seq(variants)

  /* Cluster configuration used to process frequency. */
  override def cluster: ClusterDef = super.cluster.copy(
    instances = 5,
    masterInstanceType = Strategy.computeOptimized(),
    slaveInstanceType = Strategy.computeOptimized(),
    masterVolumeSizeInGB = 500,
    slaveVolumeSizeInGB = 500,
  )

  /** Unique ancestries to process. */
  private val ancestries = Seq("AA", "AF", "EA", "EU", "HS", "SA")

  /** Any new dataset needs to update all ancestries. */
  override val rules: PartialFunction[Input, Outputs] = {
    case _ => Outputs.Named(ancestries: _*)
  }

  /** For each phenotype output, process all the datasets for it. */
  override def make(output: String): Seq[JobStep] = {
    val script   = resourceUri("pipeline/frequencyanalysis/frequencyAnalysis.py")
    val ancestry = output

    // output is an ancestry
    Seq(JobStep.PySpark(script, ancestry))
  }
}
