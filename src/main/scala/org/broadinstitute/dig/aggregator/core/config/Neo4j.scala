package org.broadinstitute.dig.aggregator.core.config

import java.util.concurrent.TimeUnit

import org.neo4j.driver.v1._

/**
 * Neo4j configuration settings.
 */
final case class Neo4jConfig(url: String, user: String, password: String) {

  /**
   * The authorization token used when instantiating a new driver connection.
   */
  val auth = AuthTokens.basic(user, password)

  /**
   * Create a new Neo4J driver connection.
   */
  def newDriver(): Driver = {
    val config = Config.build
      .withConnectionTimeout(24, TimeUnit.HOURS)
      .withConnectionLivenessCheckTimeout(10, TimeUnit.MINUTES)

    GraphDatabase.driver(url, auth, config.toConfig)
  }
}
