package org.broadinstitute.dig.aggregator.pipeline.overlapregions

import cats.effect.IO

import org.broadinstitute.dig.aggregator.core.{Processor, Run}
import org.broadinstitute.dig.aggregator.core.config.BaseConfig
import org.broadinstitute.dig.aggregator.pipeline.intake.IntakePipeline
import org.broadinstitute.dig.aggregator.pipeline.varianteffect.VariantEffectPipeline
import org.broadinstitute.dig.aws._
import org.broadinstitute.dig.aws.emr._

class OverlapRegionsProcessor(name: Processor.Name, config: BaseConfig) extends Processor(name, config) {

  /** Dependency processors.
    */
  override val dependencies: Seq[Processor.Name] = Seq(
    IntakePipeline.annotatedRegions,
    IntakePipeline.genePredictions,
    VariantEffectPipeline.variantListProcessor,
  )

  /** All the job scripts that need to be uploaded to AWS.
    */
  override val resources: Seq[String] = Seq(
    "pipeline/overlapregions/overlapRegions.py",
    "pipeline/overlapregions/uniqueRegions.py",
  )

  /** All the regions are processed into a single output.
    */
  override def getOutputs(input: Run.Result): Processor.OutputList = {
    input.processor match {
      case IntakePipeline.annotatedRegions            => Processor.Outputs(Seq("overlapregions/annotated_regions"))
      case IntakePipeline.genePredictions             => Processor.Outputs(Seq("overlapregions/gene_predictions"))
      case VariantEffectPipeline.variantListProcessor => Processor.Outputs(Seq("overlapregions/variants"))
    }
  }

  /** With a new variants list or new regions, need to reprocess and get a list
    * of all regions with the variants that they overlap.
    */
  override def processOutputs(outputs: Seq[String]): IO[Unit] = {
    val script = aws.uriOf("resources/pipeline/overlapregions/overlapRegions.py")
    val unique = aws.uriOf("resources/pipeline/overlapregions/uniqueRegions.py")

    // cluster configuration used to process each phenotype
    val cluster = Cluster(
      name = name.toString,
      masterInstanceType = InstanceType.c5_4xlarge,
      slaveInstanceType = InstanceType.c5_2xlarge,
      instances = 5,
      configurations = Seq(
        ApplicationConfig.sparkEnv.withConfig(ClassificationProperties.sparkUsePython3)
      )
    )

    // chromosomes to map
    val chrs = (1 to 22).map(_.toString) ++ Seq("X", "Y", "XY", "M")

    // the various types to overlap
    val joins = outputs.map {
      case "overlapregions/annotated_regions" => "--annotated-regions"
      case "overlapregions/gene_predictions"  => "--gene-predictions"
      case "overlapregions/variants"          => "--variants"
    }

    // create a job for variants and regions per chromosome
    val jobs = for (chr <- chrs; join <- joins) yield {
      Seq(JobStep.PySpark(script, join, chr))
    }

    // cluster the jobs across multiple machines
    val clusteredJobs = aws.clusterJobs(cluster, jobs)

    // wait for all the jobs to complete
    for {
      _ <- aws.waitForJobs(clusteredJobs)

      // finally, spin up one last job to get all unique regions
      job <- aws.runJob(cluster, JobStep.PySpark(unique))
      _   <- aws.waitForJob(job)
    } yield ()
  }
}
