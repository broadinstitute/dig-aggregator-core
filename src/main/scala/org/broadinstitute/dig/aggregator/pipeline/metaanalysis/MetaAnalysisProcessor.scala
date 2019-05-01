package org.broadinstitute.dig.aggregator.pipeline.metaanalysis

import cats.effect._
import cats.implicits._

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aggregator.core.config.BaseConfig
import org.broadinstitute.dig.aggregator.core.emr._
import org.broadinstitute.dig.aggregator.core.processors._

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
class MetaAnalysisProcessor(name: Processor.Name, config: BaseConfig) extends DatasetProcessor(name, config) {

  /** Topic to consume.
    */
  override val topic: String = "variants"

  /** All the job scripts that need to be uploaded to AWS.
    */
  override val resources: Seq[String] = Seq(
    "pipeline/metaanalysis/cluster-bootstrap.sh",
    "pipeline/metaanalysis/partitionVariants.py",
    "pipeline/metaanalysis/runAnalysis.py",
    "pipeline/metaanalysis/loadAnalysis.py",
    "scripts/getmerge-strip-headers.sh"
  )

  /** Take all the phenotype results from the dependencies and process them.
    */
  override def processDatasets(datasets: Seq[Dataset]): IO[Unit] = {
    val bootstrapUri = aws.uriOf("resources/pipeline/metaanalysis/cluster-bootstrap.sh")
    val sparkConf    = ApplicationConfig.sparkEnv.withConfig(ClassificationProperties.sparkUsePython3)

    // cluster definition to run jobs
    val cluster = Cluster(
      name = name.toString,
      bootstrapScripts = Seq(new BootstrapScript(bootstrapUri)),
      configurations = Seq(sparkConf)
    )

    // extract the dataset/phenotype pairs
    val pattern = raw"([^/]+)/(.*)".r
    val datasetPhenotypes = datasets.map(_.dataset).map {
      case dataset @ pattern(_, phenotype) => (phenotype, dataset)
    }

    // get the unique list of phenotypes
    val phenotypes = datasetPhenotypes.map(_._1).toList.distinct

    // create a job for each phenotype; cluster them
    val jobs          = phenotypes.distinct.map(processPhenotype)
    val clusteredJobs = aws.clusterJobs(cluster, jobs)

    // create the run to record for each phenotype
    val runs = phenotypes.map { phenotype =>
      val datasets = datasetPhenotypes.collect {
        case (p, dataset) if p == phenotype => dataset
      }

      // each phenotype's input is the list of datasets
      Run.insert(pool, name, datasets, phenotype)
    }

    // wait for all the jobs to complete, then insert all the results
    for {
      _ <- aws.waitForJobs(clusteredJobs)
      _ <- IO(logger.info("Updating database..."))
      _ <- runs.sequence
      _ <- IO(logger.info("Done"))
    } yield ()
  }

  /** Create a cluster and process a single phenotype.
    */
  private def processPhenotype(phenotype: String): Seq[JobStep] = {
    val partitionUri = aws.uriOf("resources/pipeline/metaanalysis/partitionVariants.py")
    val runUri       = aws.uriOf("resources/pipeline/metaanalysis/runAnalysis.py")
    val loadUri      = aws.uriOf("resources/pipeline/metaanalysis/loadAnalysis.py")

    // first run+load ancestry-specific and then trans-ethnic
    Seq(
      JobStep.PySpark(partitionUri, phenotype),
      JobStep.Script(runUri, "--ancestry-specific", phenotype),
      JobStep.PySpark(loadUri, "--ancestry-specific", phenotype),
      JobStep.Script(runUri, "--trans-ethnic", phenotype),
      JobStep.PySpark(loadUri, "--trans-ethnic", phenotype)
    )
  }
}
