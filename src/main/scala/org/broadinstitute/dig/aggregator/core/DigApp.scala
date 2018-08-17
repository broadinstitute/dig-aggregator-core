package org.broadinstitute.dig.aggregator.core

import cats.effect._
import cats.syntax.all._

import com.typesafe.scalalogging.Logger

import scala.util._

/**
 * This is the base class that all aggregator apps should derive from to
 * ensure that they run in a "pure" environment. It also wraps the execution
 * of code so that - if it fails - error conditions will send out email
 * notifications.
 *
 *  object Main extends DigApp {
 *    def run(opts: Opts): IO[ExitCode] = {
 *      ...
 *    }
 *  }
 */
class DigApp extends IOApp {

  /**
   * Use the class name as the application name, but strip out anything
   * that is extraneous and would be identical to all DIG apps.
   */
  val applicationName = {
    val name = getClass.getCanonicalName
    val core = getClass.getSuperclass.getCanonicalName

    // drop the final class name (only care about the package)
    val domain = name.split('.').dropRight(1)

    // zip with the core package, and drop everything identical
    val dropped = domain.zip(core.split('.')).takeWhile {
      case (a, b) => a == b
    }

    // everything left is the unique name
    domain.drop(dropped.size).mkString(".")
  }

  /**
   * Create a logger for this application.
   */
  val logger = Logger(applicationName)

  /**
   * Must be implemented by subclass object.
   */
  def run(opts: Opts): IO[ExitCode] = ???

  /**
   * Called from IOApp.main.
   */
  def run(args: List[String]): IO[ExitCode] = {
    val opts: Opts = new Opts(applicationName, args.toArray)

    logger.info(appVersionInfoString(opts))
    logger.info(aggregatorCoreVersionInfoString)

    if (opts.version()) {
      IO.pure(ExitCode.Success)
    } else {
      run(opts).guaranteeCase {
        case ExitCase.Error(err) => fail(opts, err)
        case _                   => IO(logger.info("Done"))
      }
    }
  }

  /**
   * Emails an error message out and returns failure.
   */
  private def fail(opts: Opts, err: Throwable): IO[Unit] = {
    val notifier = new Notifier(opts)

    for {
      _ <- IO(logger.error(err.getMessage))
      _ <- notifier.send(s"${opts.appName} terminated!", err.getMessage)
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
  private def appVersionInfoString(opts: Opts) = {
    getVersionInfoString(s"${opts.appName}-versionInfo.properties")
  }

  /**
   * Returns the version information for the aggregator core.
   */
  private def aggregatorCoreVersionInfoString = {
    getVersionInfoString("dig-aggregator-core-versionInfo.properties")
  }
}
