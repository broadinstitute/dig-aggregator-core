package org.broadinstitute.dig.aggregator.pipeline

import cats._
import cats.effect._
import cats.implicits._

import com.typesafe.scalalogging.LazyLogging

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
  def showWork(flags: Processor.Flags, config: BaseConfig): IO[Unit] = {
    val work = for (name <- processors) yield {
      Processor(name.toString)(config).get.showWork(flags)
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
  def run(flags: Processor.Flags, config: BaseConfig): IO[Unit] = {
    val ps = processors.map(n => n -> Processor(n.toString)(config).get).toMap

    // recursive helper function
    def runProcessors(): IO[Unit] = {
      val fetchWork = for ((name, p) <- ps)
        yield
          p.hasWork(flags).map { work =>
            if (work) Some(name) else None
          }

      // determine the list of all processors that have work
      fetchWork.toList.parSequence.map(_.flatten).flatMap { processors =>
        if (processors.size == 0) {
          IO(logger.info("Everything up to date."))
        } else {

          /*
           * If a processor that has work has a dependency that also has work,
           * then it shouldn't run yet. Only processors with no dependencies -
           * or with dependencies that have no work - should run.
           */
          val shouldRun = processors.filter { name =>
            ps(name) match {
              case r: RunProcessor => r.dependencies.forall(!processors.contains(_))
              case _               => false
            }
          }

          // run everything in parallel
          val io = shouldRun.size match {
            case 0 => IO.raiseError(new Exception("There's work to do, but nothing ran!"))
            case _ => shouldRun.map(ps(_).run(flags)).toList.parSequence
          }

          // after they finish, recursively try again
          io >> runProcessors
        }
      }
    }

    // run until no work is left
    runProcessors
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
