package org.broadinstitute.dig.aggregator.pipeline.ldclumping

import cats._
import cats.effect._
import cats.implicits._

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aggregator.core.config.BaseConfig
import org.broadinstitute.dig.aggregator.core.processors._
import org.broadinstitute.dig.aggregator.pipeline.metaanalysis

/**
  */
class LDClumpingProcessor(name: Processor.Name, config: BaseConfig) extends RunProcessor(name, config) {

  /** All the processors this processor depends on.
    */
  override val dependencies: Seq[Processor.Name] = Seq(
    metaanalysis.MetaAnalysisPipeline.metaAnalysisProcessor,
  )

  /** All the job scripts that need to be uploaded to AWS.
    */
  override val resources: Seq[String] = Seq(
    "pipeline/ldscore/runLDScoreRegression.py",
  )

  /**
    */
  override def getRunOutputs(work: Seq[Run.Result]): Map[String, Seq[String]] = {
    Map()
  }

  /** Take all the phenotype results from the dependencies and process them.
    */
  override def processResults(results: Seq[Run.Result]): IO[Unit] = {
    IO.unit
  }
}
