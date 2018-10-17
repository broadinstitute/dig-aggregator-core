package org.broadinstitute.dig.aggregator.app

import cats._
import cats.effect._
import cats.implicits._

import com.sendgrid._

import com.typesafe.scalalogging.LazyLogging

import org.broadinstitute.dig.aggregator.core.config.BaseConfig
import org.broadinstitute.dig.aggregator.core.processors.Processor
import org.broadinstitute.dig.aggregator.pipeline._

import scala.io.StdIn
import scala.util.Try

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
      val _ = Pipeline.pipelines

      // choose the run function
      val run: (String, Opts) => IO[Unit] = {
        if (opts.pipeline()) runPipeline else runProcessor
      }

      // verify reprocess request (if present)
      val io = confirmReprocess(opts).flatMap { confirm =>
        if (!confirm) IO.unit
        else
          run(opts.processor(), opts).guaranteeCase {
            case ExitCase.Error(err) => fail(opts.processor(), opts.config, err)
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
  private def confirmReprocess(flags: Processor.Flags): IO[Boolean] = {
    val warning = IO {
      logger.warn("The consumer state is being reset because the --reprocess")
      logger.warn("flag was passed on the command line.")
      logger.warn("")
      logger.warn("If this is the desired course of action, answer 'Y' at")
      logger.warn("the prompt; any other response will exit the program")
      logger.warn("before any damage is done.")

      StdIn.readLine("[y/N]: ").equalsIgnoreCase("y")
    }

    if (flags.reprocess() && flags.yes()) warning else IO.pure(true)
  }

  /**
   * Run an entire pipeline. This checks to see which processors in the it
   * have work to do, runs them, then does it all again recursively until the
   * processors have no work left.
   */
  private def runPipeline(name: String, opts: Opts): IO[Unit] = {
    Pipeline(name).map(_.run(opts, opts.config)).getOrElse {
      IO.raiseError(new Exception(s"Unknown pipeline '$name'"))
    }
  }

  /**
   * Runs a single processor by name.
   */
  private def runProcessor(name: String, opts: Opts): IO[Unit] = {
    Processor(name)(opts.config).map(_.run(opts)).getOrElse {
      IO.raiseError(new Exception(s"Unknown processor '$name'"))
    }
  }

  /**
   * Reports an exception by sending an email.
   */
  private def fail(pipeline: String, config: BaseConfig, err: Throwable): IO[Unit] = {
    val subject   = s"$pipeline terminated!"
    val client    = new SendGrid(config.sendgrid.key)
    val fromEmail = new Email(config.sendgrid.from)
    val content   = new Content("text/plain", err.getMessage)

    val ios = for (to <- config.sendgrid.emails) yield {
      val mail = new Mail(fromEmail, subject, new Email(to), content)
      val req  = new Request()

      IO {
        req.setMethod(Method.POST)
        req.setEndpoint("mail/send")
        req.setBody(mail.build)

        // send the email
        client.api(req)
      }
    }

    // send each of the emails in parallel
    ios.toList.parSequence >> IO.unit
  }
}
