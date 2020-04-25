package org.broadinstitute.dig.aggregator.pipeline.gregor

import org.broadinstitute.dig.aggregator.core.Processor
import org.broadinstitute.dig.aggregator.core.Run
import org.broadinstitute.dig.aggregator.core.config.BaseConfig
import org.broadinstitute.dig.aggregator.pipeline.intake.IntakePipeline
import org.broadinstitute.dig.aws.JobStep
import org.broadinstitute.dig.aws.emr.{Cluster, InstanceType, Spark}
import cats.effect.IO
import org.broadinstitute.dig.aggregator.core.DbPool

class SortRegionsProcessor(name: Processor.Name, config: BaseConfig, pool: DbPool)
    extends Processor(name, config, pool) {

  /** Dependencies.
    */
  override val dependencies: Seq[Processor.Name] = Seq(IntakePipeline.annotatedRegions)

  /** All the job scripts that need to be uploaded to AWS.
    */
  override val resources: Seq[String] = Seq(
    "pipeline/gregor/sortRegions.py"
  )

  /** All input datasets map to a single output.
    */
  override def getOutputs(input: Run.Result): Processor.OutputList = {
    Processor.Outputs(Seq("GREGOR/regions"))
  }

  /** Take any new datasets and convert them from JSON-list to BED file
    * format with all the appropriate headers and fields. All the datasets
    * are processed together by the Spark job, so what's in the results
    * input doesn't matter.
    */
  override def processOutputs(outputs: Seq[String]): IO[Unit] = {
    val script = aws.uriOf("resources/pipeline/gregor/sortRegions.py")

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

    // run all the jobs then update the database
    aws.runJob(cluster, JobStep.PySpark(script))
  }
}
