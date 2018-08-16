package org.broadinstitute.dig.aggregator.core.config

import org.neo4j.driver.v1._

/**
 * Neo4j configuration settings.
 */
final case class Neo4j(url: String, user: String, password: String) {

  /**
   * The authorization token used when instantiating a new driver connection.
   */
  val auth = AuthTokens.basic(user, password)

  /**
   * Create a new Neo4J driver connection.
   */
  def newDriver(): Driver = GraphDatabase.driver(url, auth)
}
