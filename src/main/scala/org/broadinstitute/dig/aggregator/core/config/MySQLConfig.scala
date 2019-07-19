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

  /** Query parameters to the connection string URL.
    */
  val qs = List("useCursorFetch" -> true, "useSSL" -> false)
    .map(p => s"${p._1}=${p._2}")
    .mkString("&")

  /** Driver to use for the connection is based on the engine.
    */
  val driver: String = engine match {
    case "mysql"      => "com.mysql.jdbc.Driver"
    case "postgresql" => "org.postgresql.Driver"
    case _            => "?"
  }

  /** The connection string to use for JDBC.
    */
  val connectionString = s"jdbc:mysql://$host:$port/aggregator?$qs"
}
