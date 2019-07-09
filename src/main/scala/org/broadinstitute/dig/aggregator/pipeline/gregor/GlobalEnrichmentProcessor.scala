package org.broadinstitute.dig.aggregator.pipeline.gregor

import cats.effect._
import cats.implicits._

import java.util.UUID

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aggregator.core.config.BaseConfig
import org.broadinstitute.dig.aggregator.core.emr._

class GlobalEnrichmentProcessor(name: Processor.Name, config: BaseConfig) extends Processor(name, config) {

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

  /**
    */
  override def getRunOutputs(results: Seq[Run.Result]): Map[String, Seq[UUID]] = {
    val phenotypes = results
      .filter(_.processor == GregorPipeline.snpListProcessor)
      .map(_.output)
      .distinct

    val phenotypeInputs = results
      .filter(_.processor == GregorPipeline.snpListProcessor)
      .map(_.uuid)
      .distinct

    // updated regions
    val regionInputs = results
      .filter(_.processor == GregorPipeline.sortRegionsProcessor)
      .map(_.uuid)
      .distinct

    // the phenotypes are the output, input are phenotype + regions
    phenotypes.map { phenotype =>
      phenotype -> (phenotypeInputs ++ regionInputs)
    }.toMap
  }

  /** Run GREGOR over the results of the SNP list and regions.
    */
  override def processResults(results: Seq[Run.Result]): IO[Unit] = {
    val bootstrap = aws.uriOf("resources/pipeline/gregor/cluster-bootstrap.sh")
    val install   = aws.uriOf("resources/pipeline/gregor/installGREGOR.sh")
    val run       = aws.uriOf("resources/pipeline/gregor/runGREGOR.sh")

    // r-squared threshold ("0.2" or "0.7")
    val r2 = "0.7"

    // cluster configuration used to process each phenotype
    val cluster = Cluster(
      name = name.toString,
      masterInstanceType = InstanceType.c5_9xlarge,
      instances = 1,
      masterVolumeSizeInGB = 800,
      applications = Seq.empty,
      bootstrapScripts = Seq(new BootstrapScript(bootstrap)),
      bootstrapSteps = Seq(JobStep.Script(install, r2))
    )

    // TODO: if regions.size > 0 then phenotypes = all SNPListProcessor outputs!

    // get all the phenotypes that need processed
    val phenotypes = getRunOutputs(results).keys

    // map out ancestries to that of GREGOR/1000g
    val ancestries = List(
      "AA" -> "AFR",
      "HS" -> "AMR",
      "EA" -> "ASN",
      "EU" -> "EUR",
      "SA" -> "SAN"
    )

    // For each ancestry, for each phenotype, run GREGOR.
    val jobs = for {
      (t2dkp_ancestry, gregor_ancestry) <- ancestries
      phenotype                         <- phenotypes
    } yield Seq(JobStep.Script(run, gregor_ancestry, r2, phenotype, t2dkp_ancestry))

    // cluster the jobs so each cluster has approximately the same run time
    val clusteredJobs = aws.clusterJobs(cluster, jobs)

    // run all the jobs then update the database
    aws.waitForJobs(clusteredJobs)
  }
}
