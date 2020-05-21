package org.broadinstitute.dig.aggregator.pipeline.gregor

import org.broadinstitute.dig.aggregator.core.{Glob, Run, Stage}
import org.broadinstitute.dig.aws.JobStep

/** Gathers all the output variants from the trans-ethnic, meta-analysis results
  * and generates a unique list of SNPs for use with GREGOR.
  *
  * Inputs:
  *
  *   s3://dig-analysis-data/out/metaanalysis/ancestry-specific
  *
  * Outputs:
  *
  *   s3://dig-analysis-data/out/gregor/snp/<phenotype>/<ancestry>
  */
class SNPListStage extends Stage {

  /** All the processors this processor depends on.
    */
  override val dependencies: Seq[Run.Input.Source] = Seq(
    Run.Input.Source.Success("out/metaanalysis/trans-ethnic/"),
  )

  /** The output of MetaAnalysis is the phenotype, which is also the output
    * of this processor.
    */
  override def getOutputs(input: Run.Input): Stage.Outputs = {
    val pattern = Glob("out/metaanalysis/trans-ethnic/*/...")

    input.key match {
      case pattern(phenotype) => Stage.Outputs.Set(phenotype)
    }
  }

  /** Find all the unique SNPs from all the output of the meta-analysis processor.
    */
  override def getJob(output: String): Seq[JobStep] = {
    val script    = resourceURI("pipeline/gregor/snplist.py")
    val phenotype = output

    // each phenotype gets its own snp list output
    Seq(JobStep.PySpark(script, phenotype))
  }
}
