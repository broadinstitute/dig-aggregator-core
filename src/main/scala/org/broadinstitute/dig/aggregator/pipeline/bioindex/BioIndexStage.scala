package org.broadinstitute.dig.aggregator.pipeline.bioindex

import org.broadinstitute.dig.aggregator.core.{Glob, Run, Stage}
import org.broadinstitute.dig.aws.JobStep
import org.broadinstitute.dig.aws.emr.{ClusterDef, InstanceType}

/** After running meta-analysis or gregor, the outputs are joined together with
  * other data, sorted by locus, and written to the bio index bucket so they
  * can be queried.
  */
class BioIndexStage extends Stage {

  /** Source data to consume.
    */
  override val dependencies: Seq[Run.Input.Source] = Seq(
    Run.Input.Source.Dataset("genes/"),
    Run.Input.Source.Dataset("tissues/"),
    Run.Input.Source.Dataset("variants/"),
    Run.Input.Source.Success("out/frequencyanalysis/"),
    Run.Input.Source.Success("out/gregor/regions/sorted/"),
    Run.Input.Source.Success("out/gregor/summary/"),
    Run.Input.Source.Success("out/metaanalysis/trans-ethnic/"),
    Run.Input.Source.Success("out/metaanalysis/plots/"),
    Run.Input.Source.Success("out/transcriptionfactors/"),
    Run.Input.Source.Success("out/varianteffect/common/"),
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
  override def getOutputs(input: Run.Input): Stage.Outputs = {
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
      case genes()                 => Stage.Outputs.Set("genes")
      case tissues()               => Stage.Outputs.Set("globalEnrichment", "regions")
      case variants()              => Stage.Outputs.Set("datasets")
      case metaAnalysis(phenotype) => Stage.Outputs.Set("associations")
      case plots(phenotype)        => Stage.Outputs.Set("plots")
      case enrichment(phenotype)   => Stage.Outputs.Set("globalEnrichment")
      case regions()               => Stage.Outputs.Set("regions")
      case frequency(ancestry)     => Stage.Outputs.Set("variants")
      case transcriptionFactors()  => Stage.Outputs.Set("variants")
      case vep()                   => Stage.Outputs.Set("associations", "variants")
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
