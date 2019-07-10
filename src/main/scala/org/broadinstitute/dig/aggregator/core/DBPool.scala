package org.broadinstitute.dig.aggregator.core

import cats.effect._
import cats.implicits._

import doobie._
import doobie.implicits._
import doobie.hikari._

import org.broadinstitute.dig.aggregator.core.config.MySQLConfig

/** Hikari connection pool for MySQL. */
class DbPool(driver: String, connectionString: String, user: String, password: String) {
  import Implicits._

  /** Create a new resource for a DB transactor. */
  private val transactor: Resource[IO, HikariTransactor[IO]] =
    for {
      ce <- ExecutionContexts.fixedThreadPool[IO](32)
      te <- ExecutionContexts.cachedThreadPool[IO]
      xa <- HikariTransactor.newHikariTransactor[IO](
        driver,
        connectionString,
        user,
        password,
        ce,
        te
      )
    } yield xa

  /** Get a transactor from the pool and run a query through it. */
  def exec[A](conn: ConnectionIO[A]): IO[A] = {
    transactor.use { xa =>
      conn.transact(xa)
    }
  }
}

/** Companion object. */
object DbPool {

  /** Create a new connection pool from a MySQL configuration. */
  def fromMySQLConfig(config: MySQLConfig): DbPool = {
    new DbPool(config.driver, config.connectionString, config.user, config.password)
  }
}
