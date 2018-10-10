package org.broadinstitute.dig.aggregator.app

import cats._
import cats.effect._
import cats.implicits._

import org.broadinstitute.dig.aggregator.core.config.BaseConfig
import org.broadinstitute.dig.aggregator.core.processors.Processor
import org.broadinstitute.dig.aggregator.pipeline._

import scala.util.Try

object Main extends IOApp {

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
      Register.processors()

      // ensure the processor (or pipeline) was specified
      require(opts.processor.isSupplied, "No processor specified!")

      // lookup the processor to run by unique name
      val processor = Processor(opts.processor())(opts.config)

      // launch the processor
      val run = processor.run(opts).guaranteeCase {
        case ExitCase.Error(err) => fail(processor.name, opts.config, err)
        case _                   => IO.unit
      }

      // TODO: if !opts.only() then continue processing downstream processors!

      run >> IO.pure(ExitCode.Success)
    }
  }

  /**
   *
   */
  def fail(name: Processor.Name, config: BaseConfig, err: Throwable): IO[Unit] = {
    config.sendgrid.send(s"${name.toString} terminated!", err.getMessage)
  }
}
