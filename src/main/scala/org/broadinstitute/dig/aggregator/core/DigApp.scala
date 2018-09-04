package org.broadinstitute.dig.aggregator.core

import cats.effect._
import cats.syntax.all._

import com.typesafe.scalalogging.Logger

import java.util.Properties

import org.broadinstitute.dig.aggregator.core._

import scala.util.Success
import scala.util.Failure

/**
 * This is the base class that all aggregator apps should derive from to
 * ensure that they run in a "pure" environment. It also wraps the execution
 * of code so that - if it fails - error conditions will send out email
 * notifications.
 *
 *  object Main extends DigApp("uniqueName") {
 *    def run(opts: Opts): IO[ExitCode] = {
 *      ...
 *    }
 *  }
 *
 * BN: The `applicationName` MUST BE UNIQUE ACROSS ALL DIG APPLICATIONS as
 *     it is used as the key to many database queries!!
 */
abstract class DigApp extends IOApp {

  /**
   * The name of the class that derives from `DigApp` is the name of this
   * application. It _must_ be unique across all processor apps as it is
   * used as a key in various database queries.
   *
   * If the default version won't produce a unique name (e.g. 'Main'), then
   * it is recommended that this val be overridden.
   */
  val applicationName = getClass.getName.split('.').last.stripSuffix("$")

  /**
   * Create a logger for this application.
   */
  val logger: Logger = Logger(applicationName)

  /**
   * Must be implemented by subclass object.
   */
  def run(opts: Opts): IO[ExitCode]

  /**
   * Called from IOApp.main.
   */
  override def run(args: List[String]): IO[ExitCode] = {
    assert(!List("", "Main", "DigApp").contains(applicationName))

    // parse the command line options and load the configuration file
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
  private def appVersionInfoString(opts: Opts) =
    getVersionInfoString(s"${opts.appName}-versionInfo.properties")

  /**
   * Returns the version information for the aggregator core.
   */
  private def aggregatorCoreVersionInfoString =
    getVersionInfoString("dig-aggregator-core-versionInfo.properties")
}
