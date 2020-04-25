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
import org.broadinstitute.dig.aws.emr.{Cluster, InstanceType, Spark}
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
    IntakePipeline.tissues,
    IntakePipeline.variants,
    MetaAnalysisPipeline.metaAnalysisProcessor,
    TranscriptionFactorsPipeline.transcriptionFactorsProcessor,
    VariantEffectPipeline.commonSNPProcessor,
  )

  /** All the job scripts that need to be uploaded to AWS.
    */
  override val resources: Seq[String] = Seq(
    "pipeline/bioindex/associations.py",
    "pipeline/bioindex/datasets.py",
    "pipeline/bioindex/genes.py",
    "pipeline/bioindex/globalEnrichment.py",
    "pipeline/bioindex/regions.py",
    "pipeline/bioindex/variants.py",
  )

  /** Each ancestry gets its own output.
    */
  override def getOutputs(input: Run.Result): Processor.OutputList = {
    input.processor match {
      case IntakePipeline.variants                                    => Processor.Outputs(Seq("datasets"))
      case IntakePipeline.genes                                       => Processor.Outputs(Seq("genes"))
      case IntakePipeline.tissues                                     => Processor.Outputs(Seq("globalEnrichment", "regions"))
      case MetaAnalysisPipeline.metaAnalysisProcessor                 => Processor.Outputs(Seq("associations"))
      case GregorPipeline.globalEnrichmentProcessor                   => Processor.Outputs(Seq("globalEnrichment"))
      case GregorPipeline.sortRegionsProcessor                        => Processor.Outputs(Seq("regions"))
      case FrequencyAnalysisPipeline.frequencyProcessor               => Processor.Outputs(Seq("variants"))
      case TranscriptionFactorsPipeline.transcriptionFactorsProcessor => Processor.Outputs(Seq("variants"))
      case VariantEffectPipeline.commonSNPProcessor                   => Processor.Outputs(Seq("associations", "variants"))
    }
  }

  /** For each phenotype output, process all the datasets for it.
    */
  override def processOutputs(outputs: Seq[String]): IO[Unit] = {
    val associationsScript     = aws.uriOf("resources/pipeline/bioindex/associations.py")
    val datasetsScript         = aws.uriOf("resources/pipeline/bioindex/datasets.py")
    val genesScript            = aws.uriOf("resources/pipeline/bioindex/genes.py")
    val globalEnrichmentScript = aws.uriOf("resources/pipeline/bioindex/globalEnrichment.py")
    val regionsScript          = aws.uriOf("resources/pipeline/bioindex/regions.py")
    val variantsScript         = aws.uriOf("resources/pipeline/bioindex/variants.py")

    // cluster configuration used to process each phenotype
    val cluster = Cluster(
      name = name.toString,
      instances = 5,
      masterInstanceType = InstanceType.r5_4xlarge,
      slaveInstanceType = InstanceType.r5_2xlarge,
      masterVolumeSizeInGB = 800,
      slaveVolumeSizeInGB = 800,
      configurations = Seq(
        Spark.Env().withPython3,
        Spark.Config().withMaximizeResourceAllocation,
      )
    )

    // run the jobs
    val jobs = outputs.map {
      case "associations"     => Seq(JobStep.PySpark(associationsScript))
      case "datasets"         => Seq(JobStep.PySpark(datasetsScript))
      case "genes"            => Seq(JobStep.PySpark(genesScript))
      case "globalEnrichment" => Seq(JobStep.PySpark(globalEnrichmentScript))
      case "regions"          => Seq(JobStep.PySpark(regionsScript))
      case "variants"         => Seq(JobStep.PySpark(variantsScript))
    }

    // distribute across clusters
    aws.runJobs(cluster, jobs)
  }
}
