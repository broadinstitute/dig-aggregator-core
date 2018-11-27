package org.broadinstitute.dig.aggregator.pipeline

import cats._
import cats.effect._
import cats.implicits._

import com.typesafe.scalalogging.LazyLogging

import org.broadinstitute.dig.aggregator.core.Implicits
import org.broadinstitute.dig.aggregator.core.config.BaseConfig
import org.broadinstitute.dig.aggregator.core.processors.Processor
import org.broadinstitute.dig.aggregator.core.processors.RunProcessor

import scala.concurrent.ExecutionContext.Implicits.global

/**
 * A pipeline is an object that keeps runtime knowledge of all its processors.
 */
trait Pipeline extends LazyLogging {

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
   * Output all the work that each processor in the pipeline has to do. This
   * will only show a single level of work, and not later work that may need
   * to happen downstream.
   */
  def showWork(config: BaseConfig, reprocess: Boolean): IO[Unit] = {
    val allProcessors = processors.map(Processor(_)(config).get).toSet

    // get the set of processors that have known work
    val knownWork = Pipeline.getKnownWork(allProcessors, reprocess)

    // from that, determine the processors that can actually run
    val showWork = knownWork.flatMap { toRun =>
      val processorsToRun = Pipeline.getShouldRun(allProcessors, toRun)

      // output the work for those processors
      processorsToRun.map(_.showWork(reprocess, None)).toList.sequence
    }

    showWork >> IO.unit
  }

  /**
   * Run the entire pipeline.
   *
   * This function loops over all the processors looking for any that have
   * work to do and should run, then runs them (in parallel). Once they
   * complete, this is done again. This continues until all processors are
   * done doing their work and have nothing left to do.
   */
  def run(config: BaseConfig, reprocess: Boolean): IO[Unit] = {
    val allProcessors = processors.map(Processor(_)(config).get).toSet

    // recursive helper function
    def runProcessors(reprocess: Boolean): IO[Unit] = {
      import Implicits._

      // get a list of all processors that have known work
      val knownWork: IO[Set[Processor]] = Pipeline.getKnownWork(allProcessors, reprocess)

      // determine the list of all processors that have work
      knownWork.flatMap { toRun =>
        if (toRun.isEmpty) {
          IO(logger.info("Everything up to date."))
        } else {
          val shouldRun = Pipeline.getShouldRun(allProcessors, toRun)

          // all the processors that can run can do so in parallel
          val io = shouldRun.isEmpty match {
            case true => IO.raiseError(new Exception("There's work to do, but nothing ran!"))
            case _    => shouldRun.map(_.run(reprocess, None)).toList.parSequence
          }

          // after they finish, recursively try again (don't reprocess!)
          io >> runProcessors(false)
        }
      }
    }

    // run until no work is left
    runProcessors(reprocess)
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
  def pipelines(): Map[String, Pipeline] = Map(
    "MetaAnalysisPipeline" -> metaanalysis.MetaAnalysisPipeline,
    "LDScorePipeline"      -> ldscore.LDScorePipeline,
  )

  /**
   * Lookup a registered pipeline.
   */
  def apply(pipeline: String): Option[Pipeline] = {
    pipelines.get(pipeline)
  }

  /**
   * Given a set of processors, filter those that have work.
   */
  private def getKnownWork(processors: Set[Processor], reprocess: Boolean): IO[Set[Processor]] = {
    val work = processors.map { p =>
      p.hasWork(reprocess).map {
        case true  => Some(p)
        case false => None
      }
    }

    work.toList.sequence.map(_.flatten.toSet)
  }

  /**
   * Find all processors NOT represented in `toRun` that will run due to a
   * a dependency that is in the `toRun` set.
   */
  private def getShouldRun(allProcessors: Set[Processor], toRun: Set[Processor]): Set[Processor] = {

    // true if immediate dependency is in the `toRun` set
    def dependencyWillRun(r: Processor) = r match {
      case r: RunProcessor => r.dependencies.exists(dep => toRun.exists(_.name == dep))
      case p               => false
    }

    // find all processors not yet set to run that have a dependency that will
    val inferred = (allProcessors -- toRun).filter(dependencyWillRun)

    /*
     * If processors were inferred that will end up having work due to a
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
