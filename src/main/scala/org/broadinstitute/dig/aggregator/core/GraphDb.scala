package org.broadinstitute.dig.aggregator.core

import cats._
import cats.effect._
import cats.syntax.all._

import java.util.concurrent.TimeUnit

import org.broadinstitute.dig.aggregator.core.config.Neo4jConfig

import org.neo4j.driver.v1._

import scala.collection.JavaConverters._
import scala.concurrent.duration._

/**
 * Takes a connection pool driver to the graph database and executes a query
 * on it. Because the connections can be disconnected - or other weird things
 * can happen, this catches those situations and attempts to retry queries
 * that fail up to N times before failing, which should allow the driver to
 * reconnect.
 */
class GraphDb(config: Neo4jConfig) {
  import Utils.retry

  /** The connection pool driver. */
  private lazy val driver: Driver = {
    val auth = AuthTokens.basic(config.user, config.password)

    // set custom connection settings
    val settings = Config.build
      .withMaxConnectionLifetime(24, TimeUnit.HOURS)
      .withConnectionLivenessCheckTimeout(30, TimeUnit.MINUTES)
      .withoutEncryption

    GraphDatabase.driver(config.url, auth, settings.toConfig)
  }

  /**
   * Shutdown the driver.
   */
  def shutdown(): IO[Unit] = IO(driver.close)

  /**
   * Create a session and execute a body of code with it. If something goes
   * wrong, attempt to retry running the body with a new session.
   */
  def runWithSession[A](body: Session => IO[A]): IO[A] = {
    val io = for {
      session <- IO(driver.session)
      result  <- body(session).guarantee(IO(session.close))
    } yield result

    retry(io)
  }

  /**
   * Run a query, retry if something bad happens with exponential backoff.
   *
   * Create a session to run the query, and guarantee that the session closes
   * even if something bad happens.
   */
  def run(query: String, params: Map[String, AnyRef]): IO[StatementResult] = {
    runWithSession { session =>
      IO(session.run(query, params.asJava))
    }
  }

  /**
   * Version of run with no parameters.
   */
  def run(query: String): IO[StatementResult] = {
    runWithSession { session =>
      IO(session.run(query))
    }
  }
}
