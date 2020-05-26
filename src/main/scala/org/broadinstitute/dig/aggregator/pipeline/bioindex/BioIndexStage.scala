package org.broadinstitute.dig.aggregator.pipeline.bioindex

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws.JobStep
import org.broadinstitute.dig.aws.emr.{ClusterDef, InstanceType}

/** After running meta-analysis or gregor, the outputs are joined together with
  * other data, sorted by locus, and written to the bio index bucket so they
  * can be queried.
  */
class BioIndexStage(implicit context: Context) extends Stage {
  val genes: Input.Source      = Input.Source.Dataset("genes/")
  val tissues: Input.Source    = Input.Source.Dataset("tissues/")
  val variants: Input.Source   = Input.Source.Dataset("variants/")
  val freq: Input.Source       = Input.Source.Success("out/frequencyanalysis/")
  val regions: Input.Source    = Input.Source.Success("out/gregor/regions/sorted/")
  val gregor: Input.Source     = Input.Source.Success("out/gregor/summary/")
  val bottomLine: Input.Source = Input.Source.Success("out/metaanalysis/trans-ethnic/*/")
  val plots: Input.Source      = Input.Source.Success("out/metaanalysis/plots/*/")
  val motifs: Input.Source     = Input.Source.Success("out/transcriptionfactors/")
  val vep: Input.Source        = Input.Source.Success("out/varianteffect/common/")

  /** Input sources. */
  override val sources: Seq[Input.Source] = Seq(
    genes,
    tissues,
    variants,
    freq,
    regions,
    gregor,
    bottomLine,
    plots,
    motifs,
    vep,
  )

  /** Rules for mapping input to outputs. */
  override val rules: PartialFunction[Input, Outputs] = {
    case genes()               => Outputs.Named("genes")
    case tissues()             => Outputs.Named("globalEnrichment", "regions")
    case variants()            => Outputs.Named("datasets")
    case freq()                => Outputs.Named("variants")
    case bottomLine(phenotype) => Outputs.Named(s"associations/$phenotype")
    case plots(phenotype)      => Outputs.Named(s"plots/$phenotype")
    case regions()             => Outputs.Named("regions")
    case gregor()              => Outputs.Named("globalEnrichment")
    case motifs()              => Outputs.Named("variants")
    case vep()                 => Outputs.Named("associations", "variants")
  }

  /* Cluster configuration. */
  override def cluster: ClusterDef = super.cluster.copy(
    masterInstanceType = InstanceType.r5_4xlarge,
    slaveInstanceType = InstanceType.r5_2xlarge,
    masterVolumeSizeInGB = 800,
    slaveVolumeSizeInGB = 800,
  )

  /** For each phenotype output, process all the datasets for it.
    */
  override def make(output: String): Seq[JobStep] = {
    val associationsScript     = resourceURI("pipeline/bioindex/associations.py")
    val datasetsScript         = resourceURI("pipeline/bioindex/datasets.py")
    val genesScript            = resourceURI("pipeline/bioindex/genes.py")
    val globalEnrichmentScript = resourceURI("pipeline/bioindex/globalEnrichment.py")
    val plotsScript            = resourceURI("pipeline/bioindex/statics/plots.sh")
    val regionsScript          = resourceURI("pipeline/bioindex/regions.py")
    val variantsScript         = resourceURI("pipeline/bioindex/variants.py")

    output match {
      case "associations"     => Seq(JobStep.PySpark(associationsScript))
      case "datasets"         => Seq(JobStep.PySpark(datasetsScript))
      case "genes"            => Seq(JobStep.PySpark(genesScript))
      case "globalEnrichment" => Seq(JobStep.PySpark(globalEnrichmentScript))
      case "plots"            => Seq(JobStep.Script(plotsScript))
      case "regions"          => Seq(JobStep.PySpark(regionsScript))
      case "variants"         => Seq(JobStep.PySpark(variantsScript))
    }
  }
}
