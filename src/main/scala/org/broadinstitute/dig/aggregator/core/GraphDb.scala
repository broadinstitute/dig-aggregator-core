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
      .withConnectionLivenessCheckTimeout(2, TimeUnit.MINUTES)

    GraphDatabase.driver(config.url, auth, settings.toConfig)
  }

  /**
   * Run a query, retry if something bad happens with exponential backoff.
   */
  def run(query: String, params: Map[String, AnyRef]): IO[StatementResult] = {
    retry(IO(driver.session.run(query, params.asJava)), 10.seconds)
  }

  /**
   * Version of run with no parameters.
   */
  def run(query: String): IO[StatementResult] = {
    retry(IO(driver.session.run(query)), 10.seconds)
  }
}