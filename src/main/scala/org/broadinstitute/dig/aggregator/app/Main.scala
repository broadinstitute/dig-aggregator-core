package org.broadinstitute.dig.aggregator.app

import cats._
import cats.effect._
import cats.implicits._

import com.typesafe.scalalogging.LazyLogging

import org.broadinstitute.dig.aggregator.core.DbPool
import org.broadinstitute.dig.aggregator.core.Email
import org.broadinstitute.dig.aggregator.core.Run
import org.broadinstitute.dig.aggregator.core.processors.Processor
import org.broadinstitute.dig.aggregator.pipeline._

import scala.io.StdIn

object Main extends IOApp with LazyLogging {

  /**
    * Entry point.
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

  /**
    * Called before `run` to check if --reprocess and --yes are present, and
    * to confirm with the user that the
    */
  private def confirmReprocess(opts: Opts): IO[Boolean] = {
    val warning = IO {
      logger.warn("The database state is being reset because the --reprocess")
      logger.warn("flag was passed on the command line.")
      logger.warn("")
      logger.warn("If this is the desired course of action, answer 'Y' at")
      logger.warn("the prompt; any other response will exit the program")
      logger.warn("before any damage is done.")

      StdIn.readLine("[y/N]: ").equalsIgnoreCase("y")
    }

    if (opts.reprocess() && opts.yes()) warning else IO.pure(true)
  }

  /**
    * Run an entire pipeline until all the processors in it have no work left.
    */
  private def runPipeline(name: String, opts: Opts): IO[Unit] = {
    Pipeline(name) match {
      case Some(p) => (if (opts.yes()) p.run _ else p.showWork _)(opts.config, opts.processorOpts)
      case _       => IO.raiseError(new Exception(s"Unknown pipeline '$name'"))
    }
  }

  /**
    * Runs a single processor by name.
    */
  private def runProcessor(name: String, opts: Opts): IO[Unit] = {
    Processor(name)(opts.config) match {
      case Some(p) => (if (opts.yes()) p.run _ else p.showWork _)(opts.processorOpts)
      case _       => IO.raiseError(new Exception(s"Unknown processor '$name'"))
    }
  }

  /** Verify all the runs in a given pipeline.
    */
  private def verifyPipeline(name: String, opts: Opts): IO[Unit] = {
    Pipeline(name) match {
      case Some(_) => IO.unit // p.verify(opts.yes())
      case _       => IO.raiseError(new Exception(s"Unknown pipeline '$name'"))
    }
  }

  /** Verify all the runs for a given processor.
    */
  private def verifyProcessor(name: String, opts: Opts): IO[Unit] = {
    val pool = DbPool.fromMySQLConfig(opts.config.mysql)

    Processor(name)(opts.config) match {
      case None => IO.raiseError(new Exception(s"Unknown processor '$name'"))
      case Some(p) =>
        for {
          invalidOutputs <- Run.verifyResultsOf(pool, p)

          // create the list of actions to perform (log or actually remove)
          actions = invalidOutputs.map { output =>
            if (opts.yes()) {
              Run.delete(pool, p.name, output)
            } else {
              IO(logger.info(s"Output $output of $name is invalid and needs removed."))
            }
          }

          // perform actions
          _ <- actions.toList.sequence
        } yield {
          if (actions.isEmpty) {
            logger.info(s"All results of $name validated.")
          }
        }
    }
  }

  /**
    * Reports an exception on the log and optionally sending an email.
    */
  private def fail(pipeline: String, err: Throwable, opts: Opts): IO[Unit] = {
    if (opts.emailOnFailure()) {
      new Email(opts.config.sendgrid).send(s"$pipeline terminated", err.getMessage)
    } else {
      IO(logger.error(s"$pipeline terminated: ${err.getMessage}"))
    }
  }
}
