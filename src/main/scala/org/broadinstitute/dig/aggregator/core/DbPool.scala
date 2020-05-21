package org.broadinstitute.dig.aggregator.core

import cats.effect._

import doobie._
import doobie.implicits._
import doobie.hikari._

import scala.util.DynamicVariable

/** Hikari connection pool for MySQL. */
final case class DbPool(driver: String, connectionString: String, user: String, password: String) {
  import Implicits._

  /** Create a new resource for a DB transactor. */
  private val transactor: Resource[IO, HikariTransactor[IO]] =
    for {
      ce <- ExecutionContexts.fixedThreadPool[IO](32)
      be <- Blocker[IO]
      xa <- HikariTransactor.newHikariTransactor[IO](
        driver,
        connectionString,
        user,
        password,
        ce,
        be
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

  /** The currently connection pool. */
  val current: DynamicVariable[DbPool] = new DynamicVariable[DbPool](null)

  /** Create a connection to the database and execute a body of code with it. */
  def withConnection[A](opts: Opts)(body: => IO[A]): IO[A] = {
    val conn = opts.config.aws.rds.connection.get
    val uri  = if (opts.test()) conn.getUri("test") else conn.getUri
    val pool = DbPool(conn.driver, uri, conn.username, conn.password)

    // execute the body with the current connection
    current.withValue(pool) {
      body
    }
  }
}
