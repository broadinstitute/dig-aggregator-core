package org.broadinstitute.dig.aggregator.pipeline

import cats._
import cats.effect._
import cats.implicits._

import org.broadinstitute.dig.aggregator.core.config.BaseConfig
import org.broadinstitute.dig.aggregator.core.processors.Processor

/**
 * A pipeline is an object that keeps runtime knowledge of all its processors.
 */
trait Pipeline {

  /**
   * Inspect this pipeline for all member variables that are processor names.
   */
  private[pipeline] lazy val processors: Set[Processor.Name] = {
    val fields = getClass.getDeclaredFields
      .filter(_.getType == classOf[Processor.Name])
      .map { field =>
        field.setAccessible(true)

        // get the value
        field.get(this).asInstanceOf[Processor.Name]
      }

    fields.toSet
  }

  /**
   * Run the entire pipeline.
   *
   * This function loops over all the processors looking for any that have
   * work to do and should run, then runs them (in parallel). Once they
   * complete, this is done again. This continues until all processors are
   * done doing their work and have nothing left to do.
   */
  def run(flags: Processor.Flags, config: BaseConfig): IO[Unit] = {

    /*
     * Pseudo-code:
     *
     * // instantiate each processor in this pipeline
     * val ps = for (p <- processors) yield Processor(p)(config).get
     *
     * // determine which have work and which should run now
     * val psWithWork = for (p <- ps if p.hasWork) yield p
     * val psThatShouldRun = for (p <- psWithWork if p.shouldRun) yield p
     *
     * // run them in parallel
     * val runIOs = psThatSouldRun.map(_.run(flags))
     * val results = runIOs.toList.parSequence
     *
     * // if any processors had work to do, loop and do it all again
     * if (!psWithWork.isEmpty) {
     *   results >> run(flags, config)
     * } else {
     *   results >> IO.unit
     * }
     */

    IO.unit
  }
}

/**
 * Imports all the processors from each pipeline to ensure that the names are
 * registered and that there are no conflicts!
 */
object Pipeline {

  /**
   * The global list of all pipelines.
   */
  val pipelines: Map[String, Pipeline] = Map(
    "intake"       -> intake.IntakePipeline,
    "metaanalysis" -> metaanalysis.MetaAnalysisPipeline,
    "ldscore"      -> ldscore.LDScorePipeline,
  )

  /**
   * Lookup a registered pipeline.
   */
  def apply(pipeline: String): Option[Pipeline] = {
    pipelines.get(pipeline)
  }
}
