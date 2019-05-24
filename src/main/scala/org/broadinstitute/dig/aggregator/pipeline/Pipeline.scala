package org.broadinstitute.dig.aggregator.pipeline

import cats.effect._
import cats.implicits._
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dig.aggregator.core.{Implicits, Run}
import org.broadinstitute.dig.aggregator.core.config.BaseConfig
import org.broadinstitute.dig.aggregator.core.processors.Processor
import org.broadinstitute.dig.aggregator.core.processors.RunProcessor

/** A pipeline is an object that keeps runtime knowledge of all its processors.
  */
trait Pipeline extends LazyLogging {

  /** Inspect this pipeline for all member variables that are processor names.
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

  /** Output all the work that each processor in the pipeline has to do. This
    * will only show a single level of work, and not later work that may need
    * to happen downstream.
    */
  def showWork(config: BaseConfig, opts: Processor.Opts): IO[Unit] = {
    val allProcessors = processors.map(Processor(_)(config).get)

    // get the set of processors that have known work
    val knownWork = Pipeline.getKnownWork(allProcessors, opts)

    // from that, determine the processors that can actually run
    val showWork = knownWork.flatMap { toRun =>
      val processorsToRun = Pipeline.getShouldRun(allProcessors, toRun)

      // output the work for those processors
      processorsToRun.map(_.showWork(opts)).toList.sequence
    }

    showWork >> IO.unit
  }

  /** Run the entire pipeline.
    *
    * This function loops over all the processors looking for any that have
    * work to do and should run, then runs them (in parallel). Once they
    * complete, this is done again. This continues until all processors are
    * done doing their work and have nothing left to do.
    */
  def run(config: BaseConfig, opts: Processor.Opts): IO[Unit] = {
    val allProcessors = processors.map(Processor(_)(config).get)

    // recursive helper function
    def runProcessors(opts: Processor.Opts): IO[Unit] = {
      import Implicits._

      // get a list of all processors that have known work
      val knownWork: IO[Set[Processor[_ <: Run.Input]]] = Pipeline.getKnownWork(allProcessors, opts)

      // determine the list of all processors that have work
      knownWork.flatMap { toRun =>
        if (toRun.isEmpty) {
          IO(logger.info("Everything up to date."))
        } else {
          val shouldRun = Pipeline.getShouldRun(allProcessors, toRun)

          // all the processors that can run can do so in parallel
          val io = if (shouldRun.isEmpty) {
            IO.raiseError(new Exception("There's work to do, but nothing ran!"))
          } else {
            shouldRun.map(_.run(opts)).toList.parSequence
          }

          // after they finish, recursively try again (don't reprocess!)
          io >> runProcessors(opts.copy(reprocess = false))
        }
      }
    }

    // run until no work is left
    runProcessors(opts)
  }
}

/** Imports all the processors from each pipeline to ensure that the names are
  * registered and that there are no conflicts!
  */
object Pipeline {

  /**
    * The global list of all pipelines.
    */
  def pipelines(): Map[String, Pipeline] = Map(
    "MetaAnalysisPipeline"      -> metaanalysis.MetaAnalysisPipeline,
    "FrequencyAnalysisPipeline" -> frequencyanalysis.FrequencyAnalysisPipeline,
    "LDClumpingPipeline"        -> ldclumping.LDClumpingPipeline,
    "VariantEffectPipeline"     -> varianteffect.VariantEffectPipeline,
    "GregorPipeline"            -> gregor.GregorPipeline,
  )

  /** Lookup a registered pipeline.
    */
  def apply(pipeline: String): Option[Pipeline] = {
    pipelines().get(pipeline)
  }

  /** Given a set of processors, filter those that have work.
    */
  private def getKnownWork(processors: Set[Processor[_ <: Run.Input]],
                           opts: Processor.Opts): IO[Set[Processor[_ <: Run.Input]]] = {
    val getWork = processors.toList.map { p =>
      p.hasWork(opts).map(p -> _)
    }

    for {
      work <- getWork.sequence
    } yield work.filter(_._2).map(_._1).toSet
  }

  /** Find all processors NOT represented in `toRun` that will run due to a
    * a dependency that is in the `toRun` set.
    */
  private def getShouldRun(allProcessors: Set[Processor[_ <: Run.Input]],
                           toRun: Set[Processor[_ <: Run.Input]]): Set[Processor[_ <: Run.Input]] = {

    // true if immediate dependency is in the `toRun` set
    def dependencyWillRun(r: Processor[_ <: Run.Input]) = r match {
      case r: RunProcessor => r.dependencies.exists(dep => toRun.exists(_.name == dep))
      case _               => false
    }

    // find all processors not yet set to run that have a dependency that will
    val inferred = (allProcessors -- toRun).filter(dependencyWillRun)

    /* If processors were inferred that will end up having work due to a
     * dependency that will run, add them to the set of processors `toRun` and
     * recurse.
     *
     * Otherwise, return the set of processors in the `toRun` set that either
     * have no dependencies, or dependencies that will not run.
     */

    if (inferred.nonEmpty) {
      getShouldRun(allProcessors, toRun ++ inferred)
    } else {
      toRun.filter(!dependencyWillRun(_))
    }
  }
}
