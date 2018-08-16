package org.broadinstitute.dig.aggregator.core.config

import cats.effect._

import doobie._

/**
 * MysQL configuration settings.
 */
final case class MySQL(
    url: String,
    driver: String,
    schema: String,
    user: String,
    password: String
) {

  /**
   * Query parameters to the connection string URL.
   */
  val qs = List("useCursorFetch" -> true, "useSSL" -> false)
    .map(p => s"${p._1}=${p._2}")
    .mkString("&")

  /**
   * Create a new connection to the database for running queries.
   *
   * A connection pool here isn't really all that big a deal because queries
   * are run serially while processing Kafka messages.
   */
  def newTransactor(): Transactor[IO] = {
    val connectionString = s"jdbc:mysql://$url/$schema?$qs"

    // create the connection
    Transactor.fromDriverManager[IO](
      driver,
      connectionString,
      user,
      password,
    )
  }
}
