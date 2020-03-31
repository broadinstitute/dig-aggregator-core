package org.broadinstitute.dig.aggregator.pipeline.bioindex

import cats.effect.IO
import org.broadinstitute.dig.aggregator.core.Processor
import org.broadinstitute.dig.aggregator.core.Run
import org.broadinstitute.dig.aggregator.core.config.BaseConfig
import org.broadinstitute.dig.aggregator.pipeline.frequencyanalysis.FrequencyAnalysisPipeline
import org.broadinstitute.dig.aggregator.pipeline.gregor.GregorPipeline
import org.broadinstitute.dig.aggregator.pipeline.intake.IntakePipeline
import org.broadinstitute.dig.aggregator.pipeline.metaanalysis.MetaAnalysisPipeline
import org.broadinstitute.dig.aggregator.pipeline.transcriptionfactors.TranscriptionFactorsPipeline
import org.broadinstitute.dig.aggregator.pipeline.varianteffect.VariantEffectPipeline
import org.broadinstitute.dig.aws.JobStep
import org.broadinstitute.dig.aws.emr.ApplicationConfig
import org.broadinstitute.dig.aws.emr.ClassificationProperties
import org.broadinstitute.dig.aws.emr.Cluster
import org.broadinstitute.dig.aws.emr.InstanceType
import org.broadinstitute.dig.aggregator.core.DbPool

/** After running meta-analysis or gregor, the outputs are joined together with
  * other data, sorted by locus, and written to the bio index bucket so they
  * can be queried.
  */
class BioIndexProcessor(name: Processor.Name, config: BaseConfig, pool: DbPool) extends Processor(name, config, pool) {

  /** Source data to consume.
    */
  override val dependencies: Seq[Processor.Name] = Seq(
    FrequencyAnalysisPipeline.frequencyProcessor,
    GregorPipeline.sortRegionsProcessor,
    GregorPipeline.globalEnrichmentProcessor,
    IntakePipeline.genes,
    IntakePipeline.variants,
    MetaAnalysisPipeline.metaAnalysisProcessor,
    TranscriptionFactorsPipeline.transcriptionFactorsProcessor,
    VariantEffectPipeline.variantEffectProcessor,
  )

  /** All the job scripts that need to be uploaded to AWS.
    */
  override val resources: Seq[String] = Seq(
    "pipeline/bioindex/associations.py",
    "pipeline/bioindex/datasetAssociations.py",
    "pipeline/bioindex/datasets.py",
    "pipeline/bioindex/genes.py",
    "pipeline/bioindex/globalEnrichment.py",
    "pipeline/bioindex/phenotypeAssociations.py",
    "pipeline/bioindex/regions.py",
    "pipeline/bioindex/variants.py",
    "pipeline/bioindex/burdenBinning.py",
  )

  /** Each ancestry gets its own output.
    */
  override def getOutputs(input: Run.Result): Processor.OutputList = {
    input.processor match {
      case MetaAnalysisPipeline.metaAnalysisProcessor =>
        Processor.Outputs(Seq("BioIndex/associations", "BioIndex/phenotypeAssociations"))
      case IntakePipeline.genes                                       => Processor.Outputs(Seq("BioIndex/genes"))
      case IntakePipeline.variants                                    => Processor.Outputs(Seq("BioIndex/datasets", "BioIndex/datasetAssociations"))
      case GregorPipeline.globalEnrichmentProcessor                   => Processor.Outputs(Seq("BioIndex/globalEnrichment"))
      case GregorPipeline.sortRegionsProcessor                        => Processor.Outputs(Seq("BioIndex/regions"))
      case FrequencyAnalysisPipeline.frequencyProcessor               => Processor.Outputs(Seq("BioIndex/variants"))
      case TranscriptionFactorsPipeline.transcriptionFactorsProcessor => Processor.Outputs(Seq("BioIndex/variants"))
      case VariantEffectPipeline.variantEffectProcessor               => Processor.Outputs(Seq("BioIndex/variants"))
    }
  }

  /** For each phenotype output, process all the datasets for it.
    */
  override def processOutputs(outputs: Seq[String]): IO[Unit] = {
    val associationsScript          = aws.uriOf("resources/pipeline/bioindex/associations.py")
    val datasetAssociationsScript   = aws.uriOf("resources/pipeline/bioindex/datasetAssociations.py")
    val datasetsScript              = aws.uriOf("resources/pipeline/bioindex/datasets.py")
    val genesScript                 = aws.uriOf("resources/pipeline/bioindex/genes.py")
    val globalEnrichmentScript      = aws.uriOf("resources/pipeline/bioindex/globalEnrichment.py")
    val phenotypeAssociationsScript = aws.uriOf("resources/pipeline/bioindex/phenotypeAssociations.py")
    val regionsScript               = aws.uriOf("resources/pipeline/bioindex/regions.py")
    val variantsScript              = aws.uriOf("resources/pipeline/bioindex/variants.py")
    val burdenBinningScript         = aws.uriOf("resources/pipeline/bioindex/burdenBinning.py")

    // cluster configuration used to process each phenotype
    val cluster = Cluster(
      name = name.toString,
      instances = 5,
      masterInstanceType = InstanceType.r5_4xlarge,
      slaveInstanceType = InstanceType.r5_2xlarge,
      masterVolumeSizeInGB = 800,
      slaveVolumeSizeInGB = 800,
      configurations = Seq(
        ApplicationConfig.sparkEnv.withConfig(ClassificationProperties.sparkUsePython3),
        ApplicationConfig.sparkMaximizeResourceAllocation,
      )
    )

    // run the jobs
    val jobs = outputs.map {
      case "BioIndex/associations"          => Seq(JobStep.PySpark(associationsScript))
      case "BioIndex/datasetAssociations"   => Seq(JobStep.PySpark(datasetAssociationsScript))
      case "BioIndex/datasets"              => Seq(JobStep.PySpark(datasetsScript))
      case "BioIndex/genes"                 => Seq(JobStep.PySpark(genesScript))
      case "BioIndex/globalEnrichment"      => Seq(JobStep.PySpark(globalEnrichmentScript))
      case "BioIndex/phenotypeAssociations" => Seq(JobStep.PySpark(phenotypeAssociationsScript))
      case "BioIndex/regions"               => Seq(JobStep.PySpark(regionsScript))
      case "BioIndex/variants"              => Seq(JobStep.PySpark(variantsScript))
      case "BioIndex/burdenBinning"         => Seq(JobStep.PySpark(burdenBinningScript))
    }

    // distribute across clusters
    val clusteredJobs = aws.clusterJobs(cluster, jobs)

    aws.waitForJobs(clusteredJobs)
  }
}
