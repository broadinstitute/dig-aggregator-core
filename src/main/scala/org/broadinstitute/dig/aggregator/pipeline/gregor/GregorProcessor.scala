package org.broadinstitute.dig.aggregator.pipeline.gregor

import cats.effect._
import cats.implicits._

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aggregator.core.config.BaseConfig
import org.broadinstitute.dig.aggregator.core.emr._
import org.broadinstitute.dig.aggregator.core.processors._

class GregorProcessor(name: Processor.Name, config: BaseConfig) extends RunProcessor(name, config) {

  /** All the processors this processor depends on.
    */
  override val dependencies: Seq[Processor.Name] = Seq(
    GregorPipeline.sortRegionsProcessor,
    GregorPipeline.snpListProcessor,
  )

  /** All the job scripts that need to be uploaded to AWS.
    */
  override val resources: Seq[String] = Seq(
    "scripts/getmerge-strip-headers.sh",
    "pipeline/gregor/cluster-bootstrap.sh",
    "pipeline/gregor/installGREGOR.sh",
    "pipeline/gregor/runGREGOR.sh"
  )

  /** Run GREGOR over the results of the SNP list and regions.
    */
  override def processResults(results: Seq[Run.Result]): IO[Unit] = {
    val bootstrap = aws.uriOf("resources/pipeline/gregor/cluster-bootstrap.sh")
    val install   = aws.uriOf("resources/pipeline/gregor/installGREGOR.sh")
    val run       = aws.uriOf("resources/pipeline/gregor/runGREGOR.sh")

    // cluster configuration used to process each phenotype
    val cluster = Cluster(
      name = name.toString,
      masterInstanceType = InstanceType.c5_4xlarge,
      instances = 1,
      masterVolumeSizeInGB = 800,
      applications = Seq.empty,
      bootstrapScripts = Seq(new BootstrapScript(bootstrap))
    )

    /*
    // determine if the SNP list changed
    val snpListUpdated = results.exists(_.app == GregorPipeline.snpListProcessor)

    // if the SNP list changed, then ALL regions need updating
    val getRegions: IO[Seq[Run.Result]] = if (snpListUpdated) {
      Run.resultsOf(pool, Seq(GregorPipeline.aggregateRegionsProcessor), None)
    } else {
      IO.pure(results)
    }
     */

    val ancestries = List(
      "AA" -> "AFR",
      "HS" -> "AMR",
      "EA" -> "ASN",
      "EU" -> "EUR",
      "SA" -> "SAN"
    )

    // r-squared threshold ("0.2" or "0.7")
    val r2 = "0.7"

    // NOTE: Do not cluster these jobs! Each one needs to run on a separate VM
    //       so that each ancestry can be downloaded once (per VM) and reused
    //       as opposed to having to download ALL the ancestry REF files on
    //       all the VMs just to handle all possible cases.

    // create a single job for each ancestry supported by GREGOR
    val jobs = ancestries.map {
      case (_, ancestry) =>
        val steps = Seq(
          JobStep.Script(install, ancestry, r2),
          JobStep.Script(run, ancestry, r2)
        )

        aws.runJob(cluster, steps)
    }

    // create the run for all the inputs
    val runs = ancestries.map {
      case (ancestry, _) =>
        Run.insert(pool, name, results.map(_.output), s"GREGOR/$ancestry")
    }

    // run all the jobs then update the database
    for {
      _ <- aws.waitForJobs(jobs)
      _ <- IO(logger.info("Updating database..."))
      _ <- runs.sequence
      _ <- IO(logger.info("Done"))
    } yield ()
  }
}
