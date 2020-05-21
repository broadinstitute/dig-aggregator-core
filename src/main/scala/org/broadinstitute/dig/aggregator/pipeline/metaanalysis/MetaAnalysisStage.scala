package org.broadinstitute.dig.aggregator.pipeline.metaanalysis

import org.broadinstitute.dig.aggregator.core.{Stage, Run}
import org.broadinstitute.dig.aws.JobStep
import org.broadinstitute.dig.aws.emr.{BootstrapScript, ClusterDef}

/** After all the variants for a particular phenotype have been processed and
  * partitioned, meta-analysis is run on them.
  *
  * This process runs METAL on the common variants for each ancestry (grouped
  * by dataset), then merges the rare variants across all ancestries, keeping
  * only the variants with the largest N (sample size) among them and writing
  * those back out.
  *
  * Next, trans-ethnic analysis (METAL) is run across all the ancestries, and
  * the output of that is written back to HDFS.
  *
  * The output of the ancestry-specific analysis is written to:
  *
  *  s3://dig-analysis-data/out/metaanalysis/ancestry-specific/<phenotype>/ancestry=?
  *
  * The output of the trans-ethnic analysis is written to:
  *
  *  s3://dig-analysis-data/out/metaanalysis/trans-ethnic/<phenotype>
  *
  * The inputs and outputs for this processor are expected to be phenotypes.
  */
class MetaAnalysisStage extends Stage {

  /** Processor inputs.
    */
  override val dependencies: Seq[Run.Input.Source] = Seq(
    Run.Input.Source.Dataset("variants/"),
  )

  /** Additional resources to upload. */
  override def additionalResources: Seq[String] = Seq(
    "pipeline/metaanalysis/runMETAL.sh",
    "scripts/getmerge-strip-headers.sh",
  )

  /* Cluster definition to run jobs.
   */
  override def cluster: ClusterDef = super.cluster.copy(
    masterVolumeSizeInGB = 800,
    bootstrapScripts = Seq(new BootstrapScript(resourceURI("pipeline/metaanalysis/cluster-bootstrap.sh"))),
  )

  /** The phenotype of each dataset is the output.
    */
  override def getOutputs(input: Run.Input): Stage.Outputs = {
    val pattern = raw"variants/([^/]+)/([^/]+)/.*".r

    input.key match {
      case pattern(_, phenotype) => Stage.Outputs.Set(phenotype)
    }
  }

  /** Take all the phenotype results from the dependencies and process them.
    */
  override def getJob(output: String): Seq[JobStep] = {
    val partition        = resourceURI("pipeline/metaanalysis/partitionVariants.py")
    val ancestrySpecific = resourceURI("pipeline/metaanalysis/runAncestrySpecific.sh")
    val transEthnic      = resourceURI("pipeline/metaanalysis/runTransEthnic.sh")
    val loadAnalysis     = resourceURI("pipeline/metaanalysis/loadAnalysis.py")
    val phenotype        = output

    Seq(
      JobStep.PySpark(partition, phenotype),
      // ancestry-specific analysis first and load it back
      JobStep.Script(ancestrySpecific, phenotype),
      JobStep.PySpark(loadAnalysis, "--ancestry-specific", phenotype),
      // trans-ethnic next using ancestry-specific results
      JobStep.Script(transEthnic, phenotype),
      JobStep.PySpark(loadAnalysis, "--trans-ethnic", phenotype),
    )
  }
}
