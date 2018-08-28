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
 *    def run(opts: Opts): IO[ExitCode] = {
 *      ...
 *    }
 *  }
 */
abstract class DigApp extends IOApp {

  /**
   * The (unique!) name of this application, retrieved from application.properties
   * on the classpath, where it was written by SBT.
   *
   * Note: projects that contain subclasses of `DigApp` _must_ make sure an application.properties
   * file with a `name` property in it exists on the classpath.  This is easily accomplished by writing
   * the info read by `Versions` to application.properties at the same time it's recorded elsewhere by
   * SBT.
   */
  private val applicationName: String = {
    import Versions.Implicits._

    val propsFile = "application.properties"

    val propsAttempt = Versions.propsFrom(propsFile)

    val key = "name"

    val nameAttempt = for {
      props <- propsAttempt
      name  <- props.tryGetProperty(key)
    } yield name.trim

    val name = nameAttempt.get

    require(name.nonEmpty, s"'name' property in '$propsFile' must not be empty")

    name
  }

  /**
   * Create a logger for this application.
   */
  protected val logger: Logger = Logger(applicationName)

  /**
   * Must be implemented by subclass object.
   */
  def run(opts: Opts): IO[ExitCode]

  /**
   * Called from IOApp.main.
   */
  override def run(args: List[String]): IO[ExitCode] = {
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
