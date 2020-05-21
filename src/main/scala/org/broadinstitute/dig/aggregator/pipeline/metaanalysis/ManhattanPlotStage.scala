package org.broadinstitute.dig.aggregator.pipeline.metaanalysis

import java.net.URI

import org.broadinstitute.dig.aggregator.core.{Glob, Run, Stage}
import org.broadinstitute.dig.aws.JobStep
import org.broadinstitute.dig.aws.emr.{BootstrapScript, ClusterDef, InstanceType}

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
class ManhattanPlotStage extends Stage {

  /** Processor inputs.
    */
  override val dependencies: Seq[Run.Input.Source] = Seq(
    Run.Input.Source.Success("out/metaanalysis/trans-ethnic/"),
  )

  lazy val installR: URI = resourceURI("pipeline/metaanalysis/install-R.sh")

  // cluster definition to run jobs
  override def cluster: ClusterDef = super.cluster.copy(
    masterInstanceType = InstanceType.m5_4xlarge,
    instances = 1,
    bootstrapScripts = Seq(new BootstrapScript(installR)),
  )

  /** The phenotype of each input phenotype is a plot for that phenotype.
    */
  override def getOutputs(input: Run.Input): Stage.Outputs = {
    val pattern = Glob("out/metaanalysis/trans-ethnic/*/...")

    input.key match {
      case pattern(phenotype) => Stage.Outputs.Set(phenotype)
    }
  }

  /** Take all the phenotype results from the dependencies and process them.
    */
  override def getJob(output: String): Seq[JobStep] = {
    val plotScript = resourceURI("pipeline/metaanalysis/makePlot.sh")
    val _          = resourceURI("pipeline/metaanalysis/manhattan.R")
    val phenotype  = output

    // create a job for each output (phenotype)
    Seq(JobStep.Script(plotScript, phenotype))
  }
}
