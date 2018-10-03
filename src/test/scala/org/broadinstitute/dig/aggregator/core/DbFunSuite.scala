package org.broadinstitute.dig.aggregator.core

import org.scalatest.FunSuite
import doobie._
import doobie.implicits._
import cats._
import cats.implicits._
import cats.data._
import cats.effect.IO

/**
 * @author clint
 * Aug 27, 2018
 */
trait DbFunSuite extends FunSuite with ProvidesH2Transactor {

  def dbTest(name: String)(body: => Any): Unit = {
    test(name) {
      makeTables()

      body
    }
  }

  def insert[A](a: A, rest: A*)(implicit inserter: Insertable[A]): Unit = {
    (a +: rest).toList.map(inserter.insert).sequence.unsafeRunSync()
    ()
  }

  private sealed trait Insertable[A] {
    def insert(a: A): IO[_]
  }

  private object Insertable {
    implicit object CommitsAreInsertable extends Insertable[Commit] {
      override def insert(c: Commit): IO[_] = c.insert(xa)
    }

    implicit object RunsAreInsertable extends Insertable[Run] {
      override def insert(r: Run): IO[_] = r.insert(xa)
    }
  }

  def allCommits: Seq[Commit] = {
    val q = sql"SELECT `commit`,`topic`,`partition`,`offset`,`dataset` FROM `commits`".query[Commit].to[List]

    q.transact(xa).unsafeRunSync()
  }

  def allRuns: Seq[Run] = {
    val q = sql"SELECT `run`, `app`,`input`,`output` FROM `runs`".query[Run].to[List]

    q.transact(xa).unsafeRunSync()
  }

  private def makeTables(): Unit = {
    import DbFunSuite._

    Tables.all.foreach(dropAndCreate(xa))
  }
}

object DbFunSuite {
  private def dropAndCreate(xa: Transactor[IO])(table: Table): Unit = {
    (table.drop, table.create).mapN(_ + _).transact(xa).unsafeRunSync()
    ()
  }

  private abstract class Table(name: String) {
    val drop: ConnectionIO[Int] = (fr"DROP TABLE IF EXISTS " ++ Fragment.const(name)).update.run

    def create: ConnectionIO[Int]
  }

  private object Tables {
    val all: Seq[Table] = Seq(Commits, Runs, Provenance)

    object Commits extends Table("commits") {
      override val create: ConnectionIO[Int] =
        sql"""|CREATE TABLE `commits` (
              |  `ID` int(11) NOT NULL AUTO_INCREMENT,
              |  `commit` int(64) NOT NULL,
              |  `topic` varchar(180) NOT NULL,
              |  `partition` int(11) NOT NULL,
              |  `offset` int(64) NOT NULL,
              |  `dataset` varchar(180) NOT NULL,
              |  `timestamp` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
              |  PRIMARY KEY (`ID`),
              |  UNIQUE KEY `SOURCE_IDX` (`topic`,`dataset`)
              |)
              |""".stripMargin.update.run
    }

    object Runs extends Table("runs") {
      override val create: ConnectionIO[Int] =
        sql"""|CREATE TABLE `runs` (
              |  `ID` int(11) NOT NULL AUTO_INCREMENT,
              |  `run` int(11) NOT NULL,
              |  `app` varchar(180) NOT NULL,
              |  `input` varchar(800) NOT NULL,
              |  `output` varchar(800) NOT NULL,
              |  `timestamp` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
              |  PRIMARY KEY (`ID`),
              |  UNIQUE KEY `APP_IDX` (`app`,`input`),
              |  KEY `RUN_IDX` (`run`, `app`)
              |)
              |""".stripMargin.update.run
    }

    object Provenance extends Table("provenance") {
      override val create: ConnectionIO[Int] =
        sql"""|CREATE TABLE `provenance` (
              |  `ID` int(11) NOT NULL AUTO_INCREMENT,
              |  `run` int(11) NOT NULL,
              |  `app` varchar(180) NOT NULL,
              |  `source` varchar(1024) DEFAULT NULL,
              |  `branch` varchar(180) DEFAULT NULL,
              |  `commit` varchar(40) DEFAULT NULL,
              |  PRIMARY KEY (`ID`),
              |  UNIQUE KEY `ID_UNIQUE` (`ID`),
              |  KEY `RUN_KEY_idx` (`run`,`app`),
              |  CONSTRAINT `RUN_KEY` FOREIGN KEY (`run`, `app`) REFERENCES `runs` (`run`, `app`) ON DELETE CASCADE ON UPDATE NO ACTION
              |)
              |""".stripMargin.update.run
    }
  }
}
