package org.broadinstitute.dig.aggregator.core

import cats._
import cats.data._
import cats.effect._
import cats.implicits._

import doobie._
import doobie.implicits._

import org.broadinstitute.dig.aggregator.core.processors.Processor

import org.scalatest.FunSuite

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
  }

  def allCommits: Seq[Commit] = {
    val q = sql"SELECT `commit`,`topic`,`partition`,`offset`,`dataset` FROM `commits`".query[Commit].to[List]

    q.transact(xa).unsafeRunSync()
  }

  def insertRun(app: Processor.Name, inputs: Seq[String], output: String): Long = {
    Run.insert(xa, app, inputs, output).unsafeRunSync
  }

  def allResults: Seq[Run.Result] = {
    val q = sql"""|SELECT `app`,`output`,`timestamp`
                  |FROM   `runs`
                  |""".stripMargin.query[Run.Result].to[Seq]

    q.transact(xa).unsafeRunSync
  }

  def runResults(run: Long): Seq[Run.Result] = {
    Run.results(xa, run).unsafeRunSync
  }

  def allProvenance: Seq[(Long, Processor.Name)] = {
    val q = sql"SELECT `run`, `app` FROM `provenance`".query[(Long, Processor.Name)].to[Seq]

    q.transact(xa).unsafeRunSync
  }

  def runProvenance(run: Long, app: Processor.Name): Seq[Provenance] = {
    Provenance.ofRun(xa, run, app).unsafeRunSync
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
              |  `run` bigint(20) NOT NULL,
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
      override val create: ConnectionIO[Int] = {
        /*
         * A final, last line of this CREATE TABLE command should be:
         *
         *   FOREIGN KEY (`run`, `app`) REFERENCES `runs` (`run`, `app`) ON DELETE CASCADE ON UPDATE NO ACTION
         *
         * This is valid H2 syntax (identical to MySQL), but the constraint
         * causes the tests to fail. The code queries WORK as expected on MySQL
         * in production, just not in the tests. More work should be done to
         * fix this when possible, because it having the constraint would allow
         * for run delete testing, and ensuring that the appropriate provenance
         * rows are also deleted.
         */

        sql"""|CREATE TABLE `provenance` (
              |  `ID` int(11) NOT NULL AUTO_INCREMENT,
              |  `run` bigint(20) NOT NULL,
              |  `app` varchar(180) NOT NULL,
              |  `source` varchar(1024) DEFAULT NULL,
              |  `branch` varchar(180) DEFAULT NULL,
              |  `commit` varchar(40) DEFAULT NULL,
              |  PRIMARY KEY (`ID`),
              |  UNIQUE KEY `ID_UNIQUE` (`ID`),
              |  KEY `RUN_KEY_idx` (`run`,`app`),
              |)
              |""".stripMargin.update.run
      }
    }
  }
}
