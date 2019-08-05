package org.broadinstitute.dig.aggregator.core.config

/** MysQL configuration settings.
  */
final case class MySQLConfig(
    host: String,
    port: Int,
    engine: String,
    username: String,
    password: String
) {
  require(engine == "mysql")

  /** Query parameters to the connection string URL.
    */
  val qs: String = List("useCursorFetch" -> true, "useSSL" -> false)
    .map(p => s"${p._1}=${p._2}")
    .mkString("&")

  /** Driver to use for the connection, by default assume MySQL.
    */
  val driver: String = engine match {
    case "mysql" => "com.mysql.cj.jdbc.Driver"
    case _       => "com.mysql.cj.jdbc.Driver"
  }

  /** The connection string to use for JDBC.
    */
  val connectionString = s"jdbc:$engine://$host:$port/aggregator?$qs"
}
