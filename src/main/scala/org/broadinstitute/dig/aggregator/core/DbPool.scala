package org.broadinstitute.dig.aggregator.core

import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import io.getquill._
import org.broadinstitute.dig.aws.config.RdsConfig

/** Hikari connection pool for MySQL. */
final case class DbPool(connectionString: String, engine: String, username: String, password: String) {

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

/** Companion object for creating database connection pools */
object DbPool {

  /** Create a connection pool from an AWS secret using a different schema. */
  def fromSecret(secret: RdsConfig.Secret, schema: String): DbPool = {
    DbPool(secret.connectionString(schema), secret.engine, secret.username, secret.password)
  }

  /** Create a connection pool from a AWS secret for an RDS instance. */
  def fromSecret(secret: RdsConfig.Secret): DbPool = {
    DbPool(secret.connectionString, secret.engine, secret.username, secret.password)
  }
}
