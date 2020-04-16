package org.broadinstitute.dig.aggregator.app

import cats.effect._
import cats.implicits._
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dig.aggregator.core.{DbPool, Email, Processor, Run}
import org.broadinstitute.dig.aggregator.pipeline._

import scala.io.StdIn

object Main extends IOApp with LazyLogging {

  /** Entry point.
    */
  override def run(args: List[String]): IO[ExitCode] = {
    val opts = new Opts(args)

    // TODO: show version information

    // run processor
    if (opts.version()) {
      IO.pure(ExitCode.Success)
    } else {
      // side-effect code that registers all the pipelines and processors
      Pipeline.pipelines()

      // choose the run function
      val run: (String, Opts) => IO[Unit] = {
        (opts.pipeline(), opts.verifyAndFix()) match {
          case (true, false)  => runPipeline
          case (true, true)   => verifyPipeline
          case (false, false) => runProcessor
          case (false, true)  => verifyProcessor
        }
      }

      // verify reprocess request (if present)
      val io = confirmReprocess(opts).flatMap { confirm =>
        if (!confirm) IO.unit
        else
          run(opts.processor(), opts).guaranteeCase {
            case ExitCase.Error(err) => fail(opts.processor(), err, opts)
            case _                   => IO.unit
          }
      }

      io >> IO.pure(ExitCode.Success)
    }
  }

  /** Called before `run` to check if --reprocess and --yes are present, and
    * to confirm with the user that the
    */
  private def confirmReprocess(opts: Opts): IO[Boolean] = {
    val warning = IO {
      logger.warn("The --reprocess flag was provided. All inputs")
      logger.warn("will be processed again regardless of whether")
      logger.warn("or not they already have been.")
      logger.warn("")
      logger.warn("If this is the desired course of action, answer")
      logger.warn(" Y' at the prompt; any other response will exit")
      logger.warn("before any damage is done.")

      StdIn.readLine("[y/N]: ").equalsIgnoreCase("y")
    }

    if (opts.reprocess() && opts.yes()) warning else IO.pure(true)
  }

  private def makeDefaultDbPool(opts: Opts): DbPool = DbPool.fromMySQLConfig(opts.config.mysql)

  /** Run an entire pipeline until all the processors in it have no work left.
    */
  private def runPipeline(name: String, opts: Opts): IO[Unit] = {
    def pool = makeDefaultDbPool(opts)

    Pipeline(name) match {
      case Some(p) => (if (opts.yes()) p.run _ else p.showWork _)(opts.config, pool, opts.processorOpts)
      case _       => IO.raiseError(new Exception(s"Unknown pipeline '$name'"))
    }
  }

  /** Runs a single processor by name.
    */
  private def runProcessor(name: String, opts: Opts): IO[Unit] = {
    val pool = makeDefaultDbPool(opts)

    Processor(name)(opts.config, pool) match {
      case Some(p) => (if (opts.yes()) p.run _ else p.showWork _)(opts.processorOpts)
      case _       => IO.raiseError(new Exception(s"Unknown processor '$name'"))
    }
  }

  /** Verify all the runs in a given pipeline.
    */
  private def verifyPipeline(name: String, opts: Opts): IO[Unit] = {
    Pipeline(name) match {
      case Some(p) => p.processors.map(n => verifyProcessor(n.toString, opts)).toList.sequence.as(())
      case _       => IO.raiseError(new Exception(s"Unknown pipeline '$name'"))
    }
  }

  /** Verify all the runs for a given processor.
    */
  private def verifyProcessor(name: String, opts: Opts): IO[Unit] = {
    val pool = makeDefaultDbPool(opts)

    Processor(name)(opts.config, pool) match {
      case None => IO.raiseError(new Exception(s"Unknown processor '$name'"))
      case Some(p) =>
        for {
          invalidRuns <- Run.verifyRuns(pool, p.name)

          // either actually fix or just show what needs to be fixed
          doFix = if (opts.yes()) {
            invalidRuns.map { run =>
              IO(println(s"Run $run is invalid; deleting...")) >> Run.deleteRun(pool, run)
            }
          } else {
            invalidRuns.map(run => IO(println(s"Run $run is invalid and needs removed!")))
          }

          // run all the "fix" commands
          _ <- doFix.toList.sequence
        } yield {
          if (invalidRuns.isEmpty) {
            println(s"All results of $name are good.")
          }
        }
    }
  }

  /** Reports an exception on the log and optionally sending an email.
    */
  private def fail(pipeline: String, err: Throwable, opts: Opts): IO[Unit] = {
    opts.emailOnFailure.toOption match {
      case Some(to) => Email.send(to, s"$pipeline terminated", err.getMessage)
      case None     => IO(logger.error(s"$pipeline terminated: ${err.getMessage}"))
    }
  }
}
