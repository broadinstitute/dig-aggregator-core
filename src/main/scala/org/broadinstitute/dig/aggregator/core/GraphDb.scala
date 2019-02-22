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
      .withConnectionLivenessCheckTimeout(10, TimeUnit.MINUTES)
      .withoutEncryption

    GraphDatabase.driver(config.url, auth, settings.toConfig)
  }

  /**
   * Run a query, retry if something bad happens with exponential backoff.
   *
   * Create a session to run the query, and guarantee that the session closes
   * even if something bad happens.
   */
  def run(query: String, params: Map[String, AnyRef]): IO[StatementResult] = {
    for {
      session <- IO(driver.session)

      // try multiple times to complete the operation, but always close the session
      result <- retry(IO(session.run(query, params.asJava)))
        .guarantee { IO(session.close) }
    } yield result
  }

  /**
   * Version of run with no parameters.
   */
  def run(query: String): IO[StatementResult] = run(query, Map.empty)
}
