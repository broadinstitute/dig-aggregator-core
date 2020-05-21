package org.broadinstitute.dig.aggregator.core

import cats.effect._
import cats.implicits._
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dig.aws.AWS

import scala.io.StdIn

/** A method is an object that is broken down into one or more stages and
  * run on data that has been loaded into S3, producing new data, which
  * may be the input to other methods.
  */
abstract class Method extends IOApp with LazyLogging {

  /** All the stages of this method. */
  private var stages = List.empty[Stage]

  /** Called right at startup, before the method is actually run.
    *
    * Stages are added - in the order they should execute - using the
    * addStage function, from within here.
    */
  def initStages(): Unit

  /** The provenance data for this method. */
  val provenance: Option[Provenance] = None

  /** Unique name of this method, which defaults to the name of the class.
    *
    * It must be unique across all methods and is (optionally) used to
    * determine the output location where stages in this method will output
    * their results.
    */
  def getName: String = getClass.getSimpleName.stripSuffix("$")

  /** Add a new stage to the method. */
  def addStage(stage: Stage): Unit = {
    stages +:= stage
  }

  /** Lookup a stage in this method by name. */
  def getStage(name: String): Option[Stage] = {
    stages.find(_.getName == name)
  }

  /** Output all the work that each processor in the pipeline has to do. This
    * will only show a single level of work, and not later work that may need
    * to happen downstream.
    */
  def showWork(opts: Opts, stage: Option[String] = None): IO[Unit] = {
    for (stagesToRun <- stagesWithWork(opts)) yield {
      if (stagesToRun.isEmpty) {
        logger.info("Method output is up to date; nothing to do.")
      } else {
        stagesToRun.filter(s => stage.getOrElse(s) == s).foreach { stage =>
          logger.info(s"Stage ${stage.getName} will be run.")
        }
      }
    }
  }

  /** Returns the list of stages that have new/updated dependencies. The
    * order of the stages returned is stable with respect to the order
    * they were added to the method.
    */
  private def stagesWithWork(opts: Opts): IO[List[Stage]] = {
    stages.map(stage => stage.hasWork(opts).map(stage -> _)).sequence.map { stages =>
      stages.flatMap {
        case (stage, true) => Some(stage)
        case _             => None
      }
    }
  }

  /** Execute a single stage of the method by name.
    */
  private def executeStage(name: String, opts: Opts): IO[Unit] = {
    getStage(name).map(_.run(opts)).getOrElse {
      IO.raiseError(new Exception(s"Unknown stage: $name"))
    }
  }

  /** Run the entire method.
    *
    * This function will determine which stages have new or updated
    * dependencies and then run them in the order they were added to
    * the method. Once those stages have completed, this function is
    * called again recursively. This repeats until there is no more
    * stages with work.
    */
  private def execute(opts: Opts): IO[Unit] = {
    stagesWithWork(opts).flatMap {
      case Nil         => IO(logger.info("Method output is up to date; nothing to do."))
      case stagesToRun => stagesToRun.map(_.run(opts)).sequence.as(()) >> execute(opts)
    }
  }

  /** Checks if both --reprocess and --yes are present. If so, prompt the
    * user to be sure this is what they want to do before executing body.
    */
  private def confirmReprocess(opts: Opts)(body: => IO[Unit]): IO[Unit] = {
    val warning = IO {
      logger.warn("The reprocess flag was passed. All stages")
      logger.warn("will be run; are you sure?")

      // only succeed if the user types in 'y'
      StdIn.readLine("[y/N]: ").equalsIgnoreCase("y")
    }

    if (opts.reprocess() && opts.yes()) {
      warning.flatMap(y => if (!y) IO.unit else body)
    } else {
      body
    }
  }

  /** Entry point to the method.
    *
    * This is - essentially - the main() method for the JVM. Your method
    * instance will inherit this class and be specified as the entry point
    * for the JAR.
    */
  override def run(args: List[String]): IO[ExitCode] = {
    val opts = new Opts(args)

    // TODO: show version information

    if (opts.version()) {
      IO.pure(ExitCode.Success)
    } else {
      initStages()

      // make this method current
      DbPool.withConnection(opts) {
        Method.withMethod(this, opts) {
          val action = confirmReprocess(opts) {

            opts.stage.toOption match {
              case Some(name) => if (opts.yes()) executeStage(name, opts) else showWork(opts, Some(name))
              case _          => if (opts.yes()) execute(opts) else showWork(opts)
            }
          }

          // succeed when the action finishes successfully
          for {
            _ <- Run.migrate(Method.pool.value)
            _ <- action
          } yield ExitCode.Success
        }
      }
    }
  }
}

/** Companion object with shared state for the currently running method.
  */
object Method {
  import scala.util.DynamicVariable

  /** Dynamic variables, global state to the currently running method. */
  val current: DynamicVariable[Method] = new DynamicVariable(null)
  val aws: DynamicVariable[AWS]        = new DynamicVariable(null)
  val pool: DynamicVariable[DbPool]    = new DynamicVariable(null)

  /** Set dynamic variables from a method and configuration being executed. */
  def withMethod[M <: Method, T](method: M, opts: Opts)(body: => T): T = {
    current.withValue(method) {
      aws.withValue(new AWS(opts.config.aws)) {
        pool.withValue(DbPool.fromOpts(opts)) {
          body
        }
      }
    }
  }
}
