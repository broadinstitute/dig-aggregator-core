package org.broadinstitute.dig.aggregator.pipeline.bioindex

import org.broadinstitute.dig.aggregator.core.{Glob, Input, Outputs, Stage}
import org.broadinstitute.dig.aws.JobStep
import org.broadinstitute.dig.aws.emr.{ClusterDef, InstanceType}

/** After running meta-analysis or gregor, the outputs are joined together with
  * other data, sorted by locus, and written to the bio index bucket so they
  * can be queried.
  */
class BioIndexStage extends Stage {

  /** Source data to consume.
    */
  override val dependencies: Seq[Input.Source] = Seq(
    Input.Source.Dataset("genes/"),
    Input.Source.Dataset("tissues/"),
    Input.Source.Dataset("variants/"),
    Input.Source.Success("out/frequencyanalysis/"),
    Input.Source.Success("out/gregor/regions/sorted/"),
    Input.Source.Success("out/gregor/summary/"),
    Input.Source.Success("out/metaanalysis/trans-ethnic/"),
    Input.Source.Success("out/metaanalysis/plots/"),
    Input.Source.Success("out/transcriptionfactors/"),
    Input.Source.Success("out/varianteffect/common/"),
  )

  /* Cluster configuration.
   */
  override def cluster: ClusterDef = super.cluster.copy(
    masterInstanceType = InstanceType.r5_4xlarge,
    slaveInstanceType = InstanceType.r5_2xlarge,
    masterVolumeSizeInGB = 800,
    slaveVolumeSizeInGB = 800,
  )

  /** Each ancestry gets its own output.
    */
  override def getOutputs(input: Input): Outputs = {
    val genes                = Glob("genes/...")
    val tissues              = Glob("tissues/...")
    val variants             = Glob("variants/...")
    val metaAnalysis         = Glob("out/metaanalysis/trans-ethnic/*/...")
    val plots                = Glob("out/metaanalysis/plots/*/...")
    val enrichment           = Glob("out/gregor/summary/*/...")
    val regions              = Glob("out/gregor/regions/sorted/...")
    val frequency            = Glob("out/frequencyanalysis/*/...")
    val transcriptionFactors = Glob("out/transcriptionfactors/...")
    val vep                  = Glob("out/varianteffect/common/...")

    input.key match {
      case genes()                 => Outputs.Named("genes")
      case tissues()               => Outputs.Named("globalEnrichment", "regions")
      case variants()              => Outputs.Named("datasets")
      case metaAnalysis(phenotype) => Outputs.Named("associations")
      case plots(phenotype)        => Outputs.Named("plots")
      case enrichment(phenotype)   => Outputs.Named("globalEnrichment")
      case regions()               => Outputs.Named("regions")
      case frequency(ancestry)     => Outputs.Named("variants")
      case transcriptionFactors()  => Outputs.Named("variants")
      case vep()                   => Outputs.Named("associations", "variants")
    }
  }

  /** For each phenotype output, process all the datasets for it.
    */
  override def getJob(output: String): Seq[JobStep] = {
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
