package org.broadinstitute.dig.aggregator.core

import cats._
import cats.data._
import cats.effect._
import cats.implicits._
import doobie._
import doobie.implicits._
import java.util.UUID
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

  def insertRun(processor: Processor.Name, output: String, inputs: NonEmptyList[UUID]): UUID = {
    Run.insert(pool, processor, output, inputs).unsafeRunSync
  }

  def allResults: Seq[Run.Result] = {
    import Run.UUIDGet // REQUIRED for doobie!

    val q = sql"""|SELECT `uuid`, `processor`, `output`, `timestamp`
                  |FROM   `runs`
                  |""".stripMargin.query[Run.Result].to[Seq]

    pool.exec(q).unsafeRunSync
  }

  def runResults(run: UUID): Seq[Run.Result] = {
    Run.resultsOfRun(pool, run).unsafeRunSync
  }

  private def makeTables(): Unit = {
    import DbFunSuite._

    Tables.all.foreach(dropAndCreate(pool))
  }
}

object DbFunSuite {
  private def dropAndCreate(pool: DBPool)(table: Table): Unit = {
    pool.exec((table.drop, table.create).mapN(_ + _)).unsafeRunSync()
    ()
  }

  private abstract class Table(name: String) {
    val drop: ConnectionIO[Int] = (fr"DROP TABLE IF EXISTS " ++ Fragment.const(name)).update.run

    def create: ConnectionIO[Int]
  }

  private object Tables {
    val all: Seq[Table] = Seq(Commits, Runs)

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
              |  `uuid` varchar(36) NOT NULL,
              |  `processor` varchar(180) NOT NULL,
              |  `input` varchar(36),
              |  `output` varchar(800) NOT NULL,
              |  `repo` varchar(800) NOT NULL,
              |  `branch` varchar(800) NOT NULL,
              |  `commit` varchar(800) NOT NULL,
              |  `timestamp` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
              |  PRIMARY KEY (`ID`),
              |  UNIQUE KEY `APP_IDX` (`processor`,`input`,`output`),
              |  KEY `RUN_IDX` (`uuid`)
              |)
              |""".stripMargin.update.run
    }
  }
}
