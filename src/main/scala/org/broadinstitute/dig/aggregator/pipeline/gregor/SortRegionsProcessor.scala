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

    // create the jobs to process each phenotype in parallel
    val jobs = datasets.map { d =>
      Seq(JobStep.PySpark(script, d.dataset))
    }

    // distribute the jobs among multiple clusters
    val clusteredJobs = aws.clusterJobs(cluster, jobs)

    // create the runs for each dataset
    val runs = datasets.map { d =>
      Run.insert(pool, name, Seq(d.dataset), s"chromatin_state/${d.dataset}")
    }

    // run all the jobs then update the database
    for {
      _ <- aws.waitForJobs(clusteredJobs)
      _ <- IO(logger.info("Updating database..."))
      _ <- runs.toList.sequence
      _ <- IO(logger.info("Done"))
    } yield ()
  }
}
