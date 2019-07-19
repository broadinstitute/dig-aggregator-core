package org.broadinstitute.dig.aggregator.core.config

/** MysQL configuration settings.
  */
final case class MySQLConfig(
    host: String,
    port: Int,
    engine: String,
    driver: String,
    username: String,
    password: String
) {

  /** Query parameters to the connection string URL.
    */
  val qs = List("useCursorFetch" -> true, "useSSL" -> false)
    .map(p => s"${p._1}=${p._2}")
    .mkString("&")

  /** The connection string to use for JDBC.
    */
  val connectionString = s"jdbc:mysql://$host:$port/aggregator?$qs"
}
