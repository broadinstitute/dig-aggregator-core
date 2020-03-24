package org.broadinstitute.dig.aggregator.pipeline.burdenbinning

import cats.effect.IO
import org.broadinstitute.dig.aggregator.core.{DbPool, Processor, Run}
import org.broadinstitute.dig.aggregator.core.config.BaseConfig
import org.broadinstitute.dig.aggregator.pipeline.intake.IntakePipeline
import org.broadinstitute.dig.aws.JobStep
import org.broadinstitute.dig.aws.emr._

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
class BurdenVariantBinningProcessor(name: Processor.Name, config: BaseConfig, pool: DbPool) extends Processor(name, config, pool) {

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
  override def processOutputs(outputs: Seq[String]): IO[Unit] = {
    val bootstrapUri = aws.uriOf("resources/pipeline/metaanalysis/cluster-bootstrap.sh")
    val sparkConf    = ApplicationConfig.sparkEnv.withConfig(ClassificationProperties.sparkUsePython3)

    // cluster definition to run jobs
    val cluster = Cluster(
      name = name.toString,
      masterInstanceType = InstanceType.c5_4xlarge,
      slaveInstanceType = InstanceType.c5_2xlarge,
      instances = 4,
      masterVolumeSizeInGB = 800,
      bootstrapScripts = Seq(new BootstrapScript(bootstrapUri)),
      configurations = Seq(sparkConf)
    )

    // get the unique list of phenotypes
    val phenotypes = outputs

    // create a job for each phenotype; cluster them
    val jobs          = phenotypes.map(processPhenotype)
    val clusteredJobs = aws.clusterJobs(cluster, jobs)

    // wait for all the jobs to complete, then insert all the results
    aws.waitForJobs(clusteredJobs)
  }

  /** Create a cluster and process a single phenotype.
    */
  private def processPhenotype(phenotype: String): Seq[JobStep] = {
    val partition        = aws.uriOf("resources/pipeline/metaanalysis/partitionVariants.py")
    val ancestrySpecific = aws.uriOf("resources/pipeline/metaanalysis/runAncestrySpecific.sh")
    val transEthnic      = aws.uriOf("resources/pipeline/metaanalysis/runTransEthnic.sh")
    val loadAnalysis     = aws.uriOf("resources/pipeline/metaanalysis/loadAnalysis.py")

    Seq(
      JobStep.PySpark(partition, phenotype),                    // add in first arg will be phenotype
      // ancestry-specific analysis first and load it back
      JobStep.Script(ancestrySpecific, phenotype),              // shell
      JobStep.PySpark(loadAnalysis, "--ancestry-specific", phenotype),
      // trans-ethnic next using ancestry-specific results
      JobStep.Script(transEthnic, phenotype),
      JobStep.PySpark(loadAnalysis, "--trans-ethnic", phenotype),
    )
  }
}
