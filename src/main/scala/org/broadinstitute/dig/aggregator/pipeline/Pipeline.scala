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
    val work = for (name <- processors) yield {
      Processor(name.toString)(config).get.showWork(reprocess, None)
    }

    work.toList.sequence >> IO.unit
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
    val ps = processors.map(n => n -> Processor(n)(config).get).toMap

    // recursive helper function
    def runProcessors(reprocess: Boolean): IO[Unit] = {
      import Implicits._

      val fetchWork = for ((name, p) <- ps) yield {
        p.hasWork(reprocess).map { work =>
          if (work) Some(name) else None
        }
      }

      // determine the list of all processors that have work
      fetchWork.toList.parSequence.map(_.flatten).flatMap { dependenciesToRun =>
        if (dependenciesToRun.isEmpty) {
          IO(logger.info("Everything up to date."))
        } else {

          /*
           * If a processor that has work has a dependency that also has work,
           * then it shouldn't run yet. Only processors with no dependencies -
           * or with dependencies that have no work - should run.
           */
          val shouldRun = dependenciesToRun.filter { name =>
            ps(name) match {
              case r: RunProcessor => r.dependencies.forall(!dependenciesToRun.contains(_))
              case _               => true
            }
          }

          // run everything in parallel
          val io = shouldRun.isEmpty match {
            case true => IO.raiseError(new Exception("There's work to do, but nothing ran!"))
            case _    => shouldRun.map(ps(_).run(reprocess, None)).toList.parSequence
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
}
