package org.broadinstitute.dig.aggregator.pipeline.metaanalysis

import org.broadinstitute.dig.aggregator.core.Processor
import org.broadinstitute.dig.aggregator.core.Run
import org.broadinstitute.dig.aggregator.core.config.BaseConfig
import org.broadinstitute.dig.aggregator.pipeline.intake.IntakePipeline
import org.broadinstitute.dig.aws.JobStep
import org.broadinstitute.dig.aws.emr.{BootstrapScript, Cluster, InstanceType, Spark}
import cats.effect.IO
import org.broadinstitute.dig.aggregator.core.DbPool

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
class ManhattanPlotProcessor(name: Processor.Name, config: BaseConfig, pool: DbPool)
    extends Processor(name, config, pool) {

  /** Processor inputs.
    */
  override val dependencies: Seq[Processor.Name] = Seq(MetaAnalysisPipeline.metaAnalysisProcessor)

  /** All the job scripts that need to be uploaded to AWS.
    */
  override val resources: Seq[String] = Seq(
    "pipeline/metaanalysis/install-R.sh",
    "pipeline/metaanalysis/makePlot.sh",
    "pipeline/metaanalysis/manhattan.R",
  )

  /** The phenotype of each input phenotype is a plot for that phenotype.
    */
  override def getOutputs(input: Run.Result): Processor.OutputList = {
    Processor.Outputs(Seq(input.output))
  }

  /** Take all the phenotype results from the dependencies and process them.
    */
  override def processOutputs(outputs: Seq[String]): IO[Unit] = {
    val installR   = aws.uriOf("resources/pipeline/metaanalysis/install-R.sh")
    val plotScript = aws.uriOf("resources/pipeline/metaanalysis/makePlot.sh")

    // cluster definition to run jobs
    val cluster = Cluster(
      name = name.toString,
      masterInstanceType = InstanceType.m5_4xlarge,
      instances = 1,
      masterVolumeSizeInGB = 100,
      bootstrapScripts = Seq(new BootstrapScript(installR)),
    )

    // create a job for each output (phenotype)
    val jobs = outputs.map { phenotype =>
      Seq(JobStep.Script(plotScript, phenotype))
    }

    // wait for all the jobs to complete, then insert all the results
    aws.runJobs(cluster, jobs)
  }
}
