package org.broadinstitute.dig.aggregator.pipeline.gregor

import org.broadinstitute.dig.aggregator.core.Processor
import org.broadinstitute.dig.aggregator.core.Run
import org.broadinstitute.dig.aggregator.core.config.BaseConfig
import org.broadinstitute.dig.aws.JobStep
import org.broadinstitute.dig.aws.emr.BootstrapScript
import org.broadinstitute.dig.aws.emr.Cluster
import org.broadinstitute.dig.aws.emr.InstanceType

import cats.effect.IO
import org.broadinstitute.dig.aggregator.core.DbPool

class GlobalEnrichmentProcessor(name: Processor.Name, config: BaseConfig, pool: DbPool)
    extends Processor(name, config, pool) {

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

  /** The outputs from the SNPListProcessor (phenotypes) are the outputs of this
    * processor, but all the sorted regions are processed with each as well.
    */
  override def getOutputs(input: Run.Result): Processor.OutputList = {
    input.processor match {
      case GregorPipeline.sortRegionsProcessor => Processor.AllOutputs
      case GregorPipeline.snpListProcessor     => Processor.Outputs(Seq(input.output))
    }
  }

  /** Run GREGOR over the results of the SNP list and regions.
    */
  override def processOutputs(outputs: Seq[String]): IO[Unit] = {
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

    // get all the phenotypes that need processed
    val phenotypes = outputs

    // map out ancestries to that of GREGOR/1000g
    val ancestries = List(
      "AA" -> "AFR",
      "HS" -> "AMR",
      "EA" -> "ASN",
      "EU" -> "EUR",
      "SA" -> "SAN"
    )

    // Get all the snp list source files in s3 and compare it against
    // the set of phenotype/ancestry pairings that need processed.
    //
    // Since GREGOR requires globally significant variants as input,
    // for many of the phenotype/ancestry pairs there is either no
    // ancestry dataset available for the phenotype or there are no
    // globally significant variants. In those cases, we'd be
    // submitting a step to EMR that returns immediately.
    //
    // In-and-of-itself this isn't bad, but due to rate limiting on
    // the AWS cluster state polling, it means we could be spending
    // most of the time running this processor spent waiting, doing
    // nothing.
    //
    // To prevent this situation, only jobs that actually have data
    // are created and added to the cluster.

    val buildJobList: IO[Seq[Seq[JobStep]]] =
      for (sources <- aws.ls("out/gregor/snp/")) yield {
        for {
          (t2dkp_ancestry, gregor_ancestry) <- ancestries
          phenotype                         <- phenotypes

          // pattern to match against the phenotype/ancestry pair in the source list
          pattern = s"out/gregor/snp/$phenotype/ancestry=$t2dkp_ancestry/"

          // do any of the sources match the pattern?
          if sources.exists(_.startsWith(pattern))
        } yield Seq(JobStep.Script(run, gregor_ancestry, r2, phenotype, t2dkp_ancestry))
      }

    for {
      jobs <- buildJobList
      _    <- aws.runJobs(cluster, jobs)
    } yield ()
  }
}
