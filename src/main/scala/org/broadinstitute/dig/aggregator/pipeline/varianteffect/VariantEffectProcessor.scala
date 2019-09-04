package org.broadinstitute.dig.aggregator.pipeline.varianteffect

import org.broadinstitute.dig.aggregator.core.Processor
import org.broadinstitute.dig.aggregator.core.Run
import org.broadinstitute.dig.aggregator.core.config.BaseConfig
import org.broadinstitute.dig.aws.JobStep
import org.broadinstitute.dig.aws.emr.BootstrapScript
import org.broadinstitute.dig.aws.emr.Cluster
import org.broadinstitute.dig.aws.emr.InstanceType

import cats.effect.IO

/**
  * Once all the distinct bi-allelic variants across all datasets have been
  * identified (VariantListProcessor) then they can be run through VEP in
  * parallel across multiple VMs.
  *
  * VEP TSV input files located at:
  *
  *  s3://dig-analysis-data/out/varianteffect/variants
  *
  * VEP output JSON written to:
  *
  *  s3://dig-analysis-data/out/varianteffect/effects
  */
class VariantEffectProcessor(name: Processor.Name, config: BaseConfig) extends Processor(name, config) {

  /**
    * All the processors this processor depends on.
    */
  override val dependencies: Seq[Processor.Name] = Seq(
    VariantEffectPipeline.variantListProcessor
  )

  /**
    * All the job scripts that need to be uploaded to AWS.
    */
  override val resources: Seq[String] = Seq(
    "pipeline/varianteffect/cluster-bootstrap.sh",
    "pipeline/varianteffect/installVEP.sh",
    "pipeline/varianteffect/runVEP.sh",
    "pipeline/varianteffect/runVEP.pl"
  )

  /** Only a single output for VEP that uses ALL variants.
    */
  override def getOutputs(input: Run.Result): Processor.OutputList = {
    Processor.Outputs(Seq("VEP/effects"))
  }

  /**
    * The results are ignored, as all the variants are refreshed and everything
    * needs to be run through VEP again.
    */
  override def processOutputs(outputs: Seq[String]): IO[Unit] = {
    val clusterBootstrap = aws.uriOf("resources/pipeline/varianteffect/cluster-bootstrap.sh")
    val installScript    = aws.uriOf("resources/pipeline/varianteffect/installVEP.sh")
    val runScript        = aws.uriOf("resources/pipeline/varianteffect/runVEP.pl")

    // definition of each VM "cluster" (of 1 machine) that will run VEP
    val cluster = Cluster(
      name = name.toString,
      masterInstanceType = InstanceType.c5_4xlarge,
      instances = 1,
      masterVolumeSizeInGB = 800,
      applications = Seq.empty,
      bootstrapScripts = Seq(new BootstrapScript(clusterBootstrap)),
      bootstrapSteps = Seq(JobStep.Script(installScript))
    )

    for {
      // delete all the existing effects for the dataset
      _ <- aws.rmdir(s"out/varianteffect/effects/")

      // get all the variant part files to process
      keys <- aws.ls(s"out/varianteffect/variants/", excludeSuccess = true)

      // only use the filename, not the entire key
      parts = keys.map(_.split('/').last)

      // split the part files into chunks that are processed in parallel
      steps = parts
        .sliding(20, 20)
        .map(JobStep.Script(runScript, _: _*))
        .toList

      // wrap each step so we have a list of jobs, each being a single step
      jobs = steps.map(Seq.apply(_))

      // distribute the jobs across many clustered machines
      clusteredJobs = aws.clusterJobs(cluster, jobs)

      // run and wait for them to finish
      _ <- IO(logger.info("Running VEP..."))
      _ <- aws.waitForJobs(clusteredJobs)
    } yield ()
  }
}
