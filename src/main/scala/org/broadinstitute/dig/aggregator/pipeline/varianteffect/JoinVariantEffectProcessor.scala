package org.broadinstitute.dig.aggregator.pipeline.varianteffect

import cats._
import cats.effect._
import cats.implicits._

import com.amazonaws.services.elasticmapreduce.model.RunJobFlowResult

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aggregator.core.config.BaseConfig
import org.broadinstitute.dig.aggregator.core.emr._
import org.broadinstitute.dig.aggregator.core.processors._

/**
 * After all the variants across all datasets have had VEP run on them in the
 * previous step, the results must be joined together. This is done by loading
 * all the resulting JSON files together, and only keeping a single output
 * per variant ID; the results of VEP are by variants and so will be identical
 * across datasets.
 *
 * The input location:
 *
 *  s3://dig-analysis-data/out/varianteffect/effects/<dataset>/<phenotype>/part-*.json
 *
 * The output location:
 *
 *  s3://dig-analysis-data/out/varianteffect/transcript_consequences/part-*.csv
 *  s3://dig-analysis-data/out/varianteffect/regulatory_features/part-*.csv
 *
 * The inputs and outputs for this processor are expected to be phenotypes.
 */
class JoinVariantEffectProcessor(name: Processor.Name, config: BaseConfig) extends RunProcessor(name, config) {

  /**
   * All the processors this processor depends on.
   */
  override val dependencies: Seq[Processor.Name] = Seq(
    VariantEffectPipeline.variantEffectProcessor
  )

  /**
   * All the job scripts that need to be uploaded to AWS.
   */
  override val resources: Seq[String] = Seq(
    "pipeline/varianteffect/joinVariantEffects.py"
  )

  /**
   * All effect results are combined together, so the results list is ignored.
   */
  override def processResults(results: Seq[Run.Result]): IO[Unit] = {
    val scriptUri = aws.uriOf("resources/pipeline/varianteffect/joinVariantEffects.py")

    val sparkConf = ApplicationConfig.sparkEnv.withProperties(
      "PYSPARK_PYTHON" -> "/usr/bin/python3"
    )

    // EMR cluster to run the job steps on
    val cluster = Cluster(
      name = name.toString,
      instances = 5,
      configurations = Seq(sparkConf)
    )

    // first run+load ancestry-specific and then trans-ethnic
    val steps = Seq(JobStep.PySpark(scriptUri))

    // collect all the outputs together into a list of distinct inputs
    val inputs = results.map(_.output).distinct

    // generate the run output
    val run = Run.insert(pool, name, inputs, "VEP")

    for {
      _   <- IO(logger.info(s"Joining variant effects..."))
      job <- aws.runJob(cluster, steps)
      _   <- aws.waitForJob(job)
      _   <- IO(logger.info("Updating database..."))
      _   <- run
      _   <- IO(logger.info("Done"))
    } yield ()
  }
}
