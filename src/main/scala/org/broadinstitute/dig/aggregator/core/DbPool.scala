package org.broadinstitute.dig.aggregator.core

import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import io.getquill._
import org.broadinstitute.dig.aws.config.RdsConfig

/** Hikari connection pool for MySQL. */
final case class DbPool(secret: RdsConfig.Secret, schema: String) {

  /** Constructor to use the default schema. */
  def this(secret: RdsConfig.Secret) = this(secret, secret.dbname)

  /** Connection configuration. */
  val config: HikariConfig = new HikariConfig()

  // initialize the configuration
  config.setJdbcUrl(secret.connectionString(schema))
  config.setDriverClassName(secret.driver)
  config.setUsername(secret.username)
  config.setPassword(secret.password)

  // improve performance of mysql (see: https://github.com/brettwooldridge/HikariCP/wiki/MySQL-Configuration)
  config.addDataSourceProperty("prepStmtCacheSize", 250)
  config.addDataSourceProperty("prepStmtCacheSqlLimit", 2048)
  config.addDataSourceProperty("cachePrepStmts", true)
  config.addDataSourceProperty("useServerPrepStmts", true)

  /** Connection data source. */
  lazy val dataSource = new HikariDataSource(config)

  /** Quill context. */
  lazy val ctx = new MysqlJdbcContext(LowerCase, dataSource)

  /** Close pool connections. */
  def close(): Unit = ctx.close()
}
