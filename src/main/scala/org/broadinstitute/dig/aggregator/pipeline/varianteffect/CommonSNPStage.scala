package org.broadinstitute.dig.aggregator.pipeline.varianteffect

import org.broadinstitute.dig.aggregator.core.{Stage, Run}
import org.broadinstitute.dig.aws.JobStep
import org.broadinstitute.dig.aws.emr.{ClusterDef, InstanceType, MemorySize, Spark}

/** After all the variants across all datasets have had VEP run on them in the
  * previous step the rsID for each variant is extracted into its own file.
  *
  * The input location:
  *
  *  s3://dig-analysis-data/out/varianteffect/effects/part-*.json
  *
  * The output location:
  *
  *  s3://dig-analysis-data/out/varianteffect/common/part-*.csv
  *
  * The inputs and outputs for this processor are expected to be phenotypes.
  */
class CommonSNPStage extends Stage {
  import MemorySize.Implicits._

  /** All the processors this processor depends on.
    */
  override val dependencies: Seq[Run.Input.Source] = Seq(
    Run.Input.Source.Success("out/varianteffect/effects/")
  )

  // EMR cluster to run the job steps on
  override def cluster: ClusterDef = super.cluster.copy(
    masterInstanceType = InstanceType.m5_4xlarge,
    slaveInstanceType = InstanceType.m5_8xlarge,
    instances = 4,
    configurations = Seq(
      Spark.Env().withPython3,
      Spark.Config().withMaximizeResourceAllocation,
      Spark.Defaults().withExecutorMemory(20.gb).withExecutorMemoryOverhead(4.gb),
      Spark.MapReduce().withMapMemory(8.gb).withReduceMemory(8.gb),
    ),
  )

  /** Only a single output for VEP that uses ALL effects.
    */
  override def getOutputs(input: Run.Input): Stage.Outputs = {
    Stage.Outputs.Set("VEP/common")
  }

  /** All effect results are combined together, so the results list is ignored.
    */
  override def getJob(output: String): Seq[JobStep] = {
    val scriptUri = resourceURI("pipeline/varianteffect/common.py")

    Seq(JobStep.PySpark(scriptUri))
  }
}
