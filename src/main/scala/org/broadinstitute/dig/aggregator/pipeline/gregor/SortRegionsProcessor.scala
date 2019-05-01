package org.broadinstitute.dig.aggregator.pipeline.gregor

import cats.effect._
import cats.implicits._

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aggregator.core.config.BaseConfig
import org.broadinstitute.dig.aggregator.core.emr._
import org.broadinstitute.dig.aggregator.core.processors._

class SortRegionsProcessor(name: Processor.Name, config: BaseConfig) extends DatasetProcessor(name, config) {

  /** Topic to consume.
    */
  override val topic: String = "chromatin_state"

  /** All the job scripts that need to be uploaded to AWS.
    */
  override val resources: Seq[String] = Seq(
    "pipeline/gregor/sortRegions.py"
  )

  /** Take any new datasets and convert them from JSON-list to BED file
    * format with all the appropriate headers and fields.
    */
  override def processDatasets(datasets: Seq[Dataset]): IO[Unit] = {
    val script = aws.uriOf("resources/pipeline/gregor/sortRegions.py")

    // cluster configuration used to process each phenotype
    val cluster = Cluster(name = name.toString)

    // all the datasets are inputs to the single output
    val inputs = datasets.map(_.dataset)

    // run all the jobs then update the database
    for {
      job <- aws.runJob(cluster, JobStep.PySpark(script))
      _   <- aws.waitForJob(job)
      _   <- IO(logger.info("Updating database..."))
      _   <- Run.insert(pool, name, inputs, s"chromatin_state")
      _   <- IO(logger.info("Done"))
    } yield ()
  }
}
