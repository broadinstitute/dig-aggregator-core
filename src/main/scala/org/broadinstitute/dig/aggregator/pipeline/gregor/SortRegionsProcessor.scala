package org.broadinstitute.dig.aggregator.pipeline.gregor

import cats.effect._

import java.util.UUID

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aggregator.core.config.BaseConfig
import org.broadinstitute.dig.aggregator.core.emr._
import org.broadinstitute.dig.aggregator.pipeline.intake.IntakePipeline

class SortRegionsProcessor(name: Processor.Name, config: BaseConfig) extends Processor(name, config) {

  /** Dependencies.
    */
  override val dependencies: Seq[Processor.Name] = Seq(IntakePipeline.chromatinState)

  /** All the job scripts that need to be uploaded to AWS.
    */
  override val resources: Seq[String] = Seq(
    "pipeline/gregor/sortRegions.py"
  )

  /** All datasets map to a single output.
    */
  override def getRunOutputs(work: Seq[Run.Result]): Map[String, Seq[UUID]] = {
    Map("regions/chromatin_state" -> work.map(_.uuid).distinct)
  }

  /** Take any new datasets and convert them from JSON-list to BED file
    * format with all the appropriate headers and fields. All the datasets
    * are processed together by the Spark job, so what's in the results
    * input doesn't matter.
    */
  override def processResults(results: Seq[Run.Result]): IO[Unit] = {
    val script = aws.uriOf("resources/pipeline/gregor/sortRegions.py")

    // cluster configuration used to process each phenotype
    val cluster = Cluster(
      name = name.toString,
      masterInstanceType = InstanceType.c5_4xlarge,
      slaveInstanceType = InstanceType.c5_2xlarge,
      instances = 5,
      configurations = Seq(
        ApplicationConfig.sparkEnv.withConfig(ClassificationProperties.sparkUsePython3)
      )
    )

    // run all the jobs then update the database
    for {
      job <- aws.runJob(cluster, JobStep.PySpark(script))
      _   <- aws.waitForJob(job)
    } yield ()
  }
}
