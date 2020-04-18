package org.broadinstitute.dig.aggregator.pipeline.overlapregions

import cats.effect.IO

import org.broadinstitute.dig.aggregator.core.{Processor, Run}
import org.broadinstitute.dig.aggregator.core.config.BaseConfig
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._
import org.broadinstitute.dig.aggregator.core.DbPool

class UniqueOverlapRegionsProcessor(name: Processor.Name, config: BaseConfig, pool: DbPool)
    extends Processor(name, config, pool) {

  /** Dependency processors.
    */
  override val dependencies: Seq[Processor.Name] = Seq(
    OverlapRegionsPipeline.overlapRegionsProcessor,
  )

  /** All the job scripts that need to be uploaded to AWS.
    */
  override val resources: Seq[String] = Seq(
    "pipeline/overlapregions/uniqueRegions.py",
  )

  /** All the regions are processed into a single output.
    */
  override def getOutputs(input: Run.Result): Processor.OutputList = {
    Processor.Outputs(Seq("overlapregions/unique"))
  }

  /** With a new variants list or new regions, need to reprocess and get a list
    * of all regions with the variants that they overlap.
    */
  override def processOutputs(outputs: Seq[String]): IO[Unit] = {
    val unique = aws.uriOf("resources/pipeline/overlapregions/uniqueRegions.py")

    // cluster configuration used to process each phenotype
    val cluster = Cluster(
      name = name.toString,
      masterInstanceType = InstanceType.c5_4xlarge,
      slaveInstanceType = InstanceType.c5_2xlarge,
      instances = 5,
      configurations = Seq(
        Spark.Env().withPython3,
      )
    )

    aws.runJob(cluster, JobStep.PySpark(unique))
  }
}
