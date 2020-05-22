package org.broadinstitute.dig.aggregator.pipeline.varianteffect

import org.broadinstitute.dig.aggregator.core.{Context, Input, Outputs, Stage}
import org.broadinstitute.dig.aws.JobStep
import org.broadinstitute.dig.aws.emr.{BootstrapScript, ClusterDef, InstanceType}

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
class VariantEffectStage extends Stage {

  /** Additional resources that need uploaded to S3. */
  override def additionalResources: Seq[String] = Seq(
    "pipeline/varianteffect/runVEP.sh",
  )

  /** All the processors this processor depends on.
    */
  override val dependencies: Seq[Input.Source] = Seq(
    Input.Source.Success("out/varianteffect/variants/"),
  )

  private lazy val clusterBootstrap = resourceURI("pipeline/varianteffect/cluster-bootstrap.sh")
  private lazy val installScript    = resourceURI("pipeline/varianteffect/installVEP.sh")

  /** Definition of each VM "cluster" (of 1 machine) that will run VEP.
    */
  override def cluster: ClusterDef = super.cluster.copy(
    masterInstanceType = InstanceType.c5_9xlarge,
    instances = 1,
    masterVolumeSizeInGB = 800,
    applications = Seq.empty,
    bootstrapScripts = Seq(new BootstrapScript(clusterBootstrap)),
    bootstrapSteps = Seq(JobStep.Script(installScript))
  )

  /** Only a single output for VEP that uses ALL variants.
    */
  override def getOutputs(input: Input): Outputs = {
    Outputs.Named("VEP")
  }

  /** The results are ignored, as all the variants are refreshed and everything
    * needs to be run through VEP again.
    */
  override def getJob(output: String): Seq[JobStep] = {
    val runScript = resourceURI("pipeline/varianteffect/runVEP.pl")
    val aws       = Context.current.aws

    // delete all existing effects
    aws.rmdir("out/varianteffect/effects/")

    // get all the variant part files to process, use only the part filename
    val objects = aws.ls(s"out/varianteffect/variants/", excludeSuccess = true)
    val keys    = objects.map(_.key)
    val parts   = keys.map(_.split('/').last)

    // each part file is a separate step to run
    parts.map(JobStep.Script(runScript, _))
  }
}
