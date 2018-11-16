package org.broadinstitute.dig.aggregator.core.config

/**
 * MysQL configuration settings.
 */
final case class MySQLConfig(
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
   * The connection string to use for JDBC.
   */
  val connectionString = s"jdbc:mysql://$url/$schema?$qs"
}
