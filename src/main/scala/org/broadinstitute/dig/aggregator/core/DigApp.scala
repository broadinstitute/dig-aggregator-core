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
 *  object Main extends DigApp {
 *    val applicationName: String = "MyApp"
 *
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
   * Define the unique name for this application.
   */
  val registeredApp: RegisteredApp

  /**
   * Create a logger for this application.
   */
  lazy val logger: Logger = Logger(registeredApp.appName)

  /**
   * Must be implemented by subclass object.
   */
  def run(opts: Opts): IO[ExitCode]

  /**
   * Called from IOApp.main.
   */
  override def run(args: List[String]): IO[ExitCode] = {
    val registeredClass = DigAppRegistry(registeredApp.appName)

    // verify that the registered class exists and matches
    registeredClass match {
      case Some(c) => require(c == getClass, s"${getClass.getName} != ${c.getName}!")
      case None    => throw new Exception(s"${registeredApp.appName} is not a registered app!")
    }

    // parse the command line options and load the configuration file
    val opts: Opts = new Opts(registeredApp.appName, args.toArray)

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
