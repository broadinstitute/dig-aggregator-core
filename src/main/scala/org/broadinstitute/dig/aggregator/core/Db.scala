package org.broadinstitute.dig.aggregator.core

import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import io.getquill._
import org.broadinstitute.dig.aws.config.RdsConfig

/** Hikari connection pool for MySQL. */
final class Db(connectionString: String, engine: String, username: String, password: String) {

  /** Create a new connection pool from a secret and a schema name */
  def this(secret: RdsConfig.Secret, schema: String) = {
    this(secret.connectionString(schema), secret.engine, secret.username, secret.password)
  }

  /** Create a new connection pool from a secret using the default schema. */
  def this(secret: RdsConfig.Secret) = {
    this(secret.connectionString, secret.engine, secret.username, secret.password)
  }

  /** Create an in-memory H2 database (mostly used for testing). */
  def this() = {
    this(s"jdbc:h2:mem:${java.util.UUID.randomUUID};DB_CLOSE_DELAY=-1;mode=MySQL", "h2", "", "")
  }

  /** Connection configuration. */
  val config: HikariConfig = new HikariConfig()

  /** Set the driver class to use. */
  val driver: String = engine match {
    case "mysql" => "com.mysql.cj.jdbc.Driver"
    case "h2"    => "org.h2.Driver"
    case _       => throw new IllegalArgumentException(s"Unhandled DB engine: $engine")
  }

  // initialize the configuration
  config.setJdbcUrl(connectionString)
  config.setDriverClassName(driver)
  config.setUsername(username)
  config.setPassword(password)

  // improve performance of mysql (see: https://github.com/brettwooldridge/HikariCP/wiki/MySQL-Configuration)
  if (driver == "mysql") {
    config.addDataSourceProperty("prepStmtCacheSize", 250)
    config.addDataSourceProperty("prepStmtCacheSqlLimit", 2048)
    config.addDataSourceProperty("cachePrepStmts", true)
    config.addDataSourceProperty("useServerPrepStmts", true)
  }

  /** Connection data source. */
  lazy val dataSource = new HikariDataSource(config)

  /** Quill context. */
  lazy val ctx = new MysqlJdbcContext(LowerCase, dataSource)

  /** Close pool connections. */
  def close(): Unit = ctx.close()
}
