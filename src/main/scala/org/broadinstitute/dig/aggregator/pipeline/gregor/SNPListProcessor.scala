package org.broadinstitute.dig.aggregator.pipeline.gregor

import org.broadinstitute.dig.aggregator.core.Processor
import org.broadinstitute.dig.aggregator.core.Run
import org.broadinstitute.dig.aggregator.core.config.BaseConfig
import org.broadinstitute.dig.aggregator.pipeline.metaanalysis.MetaAnalysisPipeline
import org.broadinstitute.dig.aws.JobStep
import org.broadinstitute.dig.aws.emr.{Cluster, InstanceType, Spark}
import cats.effect.IO
import org.broadinstitute.dig.aggregator.core.DbPool

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
class SNPListProcessor(name: Processor.Name, config: BaseConfig, pool: DbPool) extends Processor(name, config, pool) {

  /** All the processors this processor depends on.
    */
  override val dependencies: Seq[Processor.Name] = Seq(
    MetaAnalysisPipeline.metaAnalysisProcessor
  )

  /** All the job scripts that need to be uploaded to AWS.
    */
  override val resources: Seq[String] = Seq(
    "pipeline/gregor/snplist.py"
  )

  /** The output of MetaAnalysis is the phenotype, which is also the output
    * of this processor.
    */
  override def getOutputs(input: Run.Result): Processor.OutputList = {
    Processor.Outputs(Seq(input.output))
  }

  /** Find all the unique SNPs from all the output of the meta-analysis processor.
    */
  override def getJob(output: String): Seq[JobStep] = {
    val script    = aws.uriOf("resources/pipeline/gregor/snplist.py")
    val phenotype = output

    // each phenotype gets its own snp list output
    Seq(JobStep.PySpark(script, phenotype))
  }
}
