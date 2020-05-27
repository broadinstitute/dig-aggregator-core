package org.broadinstitute.dig.aggregator.pipeline.gregor

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws.JobStep
import org.broadinstitute.dig.aws.emr.{BootstrapScript, ClusterDef, InstanceType}

class GlobalEnrichmentStage(implicit context: Context) extends Stage {
  val regions: Input.Source = Input.Source.Success("out/gregor/regions/sorted/")
  val snp: Input.Source     = Input.Source.Success("out/gregor/snp/*/")

  /** All the processors this processor depends on.
    */
  override val sources: Seq[Input.Source] = Seq(regions, snp)

  /* Install scripts. */
  private lazy val bootstrap = resourceUri("pipeline/gregor/cluster-bootstrap.sh")
  private lazy val install   = resourceUri("pipeline/gregor/installGREGOR.sh")

  /* r^2 parameter to scripts */
  private val r2 = "0.7"

  // cluster configuration used to process each phenotype
  override def cluster: ClusterDef = super.cluster.copy(
    masterInstanceType = InstanceType.c5_9xlarge,
    instances = 1,
    masterVolumeSizeInGB = 800,
    applications = Seq.empty,
    bootstrapScripts = Seq(new BootstrapScript(bootstrap)),
    bootstrapSteps = Seq(JobStep.Script(install, r2))
  )

  // map internal ancestries to that of GREGOR/1000g
  private val ancestries = List(
    "AA" -> "AFR",
    "HS" -> "AMR",
    "EA" -> "ASN",
    "EU" -> "EUR",
    "SA" -> "SAN"
  )

  /** The outputs from the SNPListProcessor (phenotypes) are the outputs of this
    * processor, but all the sorted regions are processed with each as well.
    */
  override val rules: PartialFunction[Input, Outputs] = {
    case regions()      => Outputs.All
    case snp(phenotype) => Outputs.Named(phenotype)
  }

  /** Run GREGOR over the results of the SNP list and regions.
    */
  override def make(output: String): Seq[JobStep] = {
    val run       = resourceUri("pipeline/gregor/runGREGOR.sh")
    val phenotype = output

    // a phenotype needs processed per ancestry
    ancestries.map {
      case (t2dkp_ancestry, gregor_ancestry) =>
        JobStep.Script(run, gregor_ancestry, r2, phenotype, t2dkp_ancestry)
    }
  }
}
