package org.broadinstitute.dig.aggregator.core

import cats.effect._
import cats.syntax.all._

import com.typesafe.scalalogging.LazyLogging

import scala.util._

/**
 * This is the base class that all aggregator apps should derive from to
 * ensure that they run in a "pure" environment. It also wraps the execution
 * of code so that - if it fails - error conditions will send out email
 * notifications.
 *
 *  object MyDigApp extends DigApp[Config] {
 *
 *    def run(opts: Opts[Config]): IO[ExitCode] = {
 *      ...
 *    }
 *  }
 */
abstract class DigApp[C <: BaseConfig](implicit m: Manifest[C]) extends IOApp with LazyLogging {

  /**
   * Must be implemented by subclass object.
   */
  def run(opts: Opts[C]): IO[ExitCode] = ???

  /**
   * Called from IOApp
   */
  def run(args: List[String]): IO[ExitCode] = {
    val opts: Opts[C] = new Opts[C](args.toArray)

    logger.info(appVersionInfoString(opts.config.app))
    logger.info(aggregatorCoreVersionInfoString)

    if (opts.version()) {
      IO.pure(ExitCode.Success)
    } else {
      run(opts).guaranteeCase {
        case ExitCase.Error(err) => fail(opts.config, err)
        case ExitCase.Canceled   => IO(logger.error("Canceled"))
        case ExitCase.Completed  => IO(logger.info("Done"))
      }
    }
  }

  /**
   * Emails an error message out and returns failure.
   */
  private def fail(config: BaseConfig, err: Throwable): IO[Unit] = {
    val subject = s"${config.app} terminated!"

    for {
      _ <- IO(logger.error(err.getMessage))
      _ <- config.sendgrid.notify(subject, err.getMessage)
    } yield ()
  }

  /**
   * Looks up the version information in the properties file for this JAR.
   */
  private def getVersionInfoString(propsFile: String): String = {
    val versionsAttempt = Versions.load(propsFile).map(_.toString)

    versionsAttempt match {
      case Success(info) => info
      case Failure(e)    => s"Missing version from '$propsFile': '${e.getMessage}'"
    }
  }

  /**
   * Returns the version information for this application.
   */
  private def appVersionInfoString(app: String) = {
    getVersionInfoString(s"$app-versionInfo.properties")
  }

  /**
   * Returns the version information for the aggregator core.
   */
  private def aggregatorCoreVersionInfoString = {
    getVersionInfoString("dig-aggregator-core-versionInfo.properties")
  }
}
