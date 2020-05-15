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
class MetaAnalysisProcessor(name: Processor.Name, config: BaseConfig, pool: DbPool)
    extends Processor(name, config, pool) {

  /** Processor inputs.
    */
  override val dependencies: Seq[Processor.Name] = Seq(IntakePipeline.variants)

  /** All the job scripts that need to be uploaded to AWS.
    */
  override val resources: Seq[String] = Seq(
    "pipeline/metaanalysis/cluster-bootstrap.sh",
    "pipeline/metaanalysis/partitionVariants.py",
    "pipeline/metaanalysis/runMETAL.sh",
    "pipeline/metaanalysis/runAncestrySpecific.sh",
    "pipeline/metaanalysis/runTransEthnic.sh",
    "pipeline/metaanalysis/loadAnalysis.py",
    "scripts/getmerge-strip-headers.sh"
  )

  /* Cluster definition to run jobs.
   */
  override val cluster: Cluster = super.cluster.copy(
    masterVolumeSizeInGB = 800,
    bootstrapScripts = Seq(new BootstrapScript(aws.uriOf("resources/pipeline/metaanalysis/cluster-bootstrap.sh"))),
  )

  /** The phenotype of each dataset is the output.
    */
  override def getOutputs(input: Run.Result): Processor.OutputList = {
    val pattern = raw"([^/]+)/(.*)".r

    input.output match {
      case pattern(_, phenotype) => Processor.Outputs(Seq(phenotype))
    }
  }

  /** Take all the phenotype results from the dependencies and process them.
    */
  override def getJob(output: String): Seq[JobStep] = {
    val partition        = aws.uriOf("resources/pipeline/metaanalysis/partitionVariants.py")
    val ancestrySpecific = aws.uriOf("resources/pipeline/metaanalysis/runAncestrySpecific.sh")
    val transEthnic      = aws.uriOf("resources/pipeline/metaanalysis/runTransEthnic.sh")
    val loadAnalysis     = aws.uriOf("resources/pipeline/metaanalysis/loadAnalysis.py")
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
