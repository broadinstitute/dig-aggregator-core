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
  
  def insert(is: Insertable*): Unit = {
    is.toList.map(_.insert(xa)).sequence.unsafeRunSync()
  }
  
  def allCommits: Seq[Commit] = { 
    val q = sql"SELECT `commit`,`topic`,`partition`,`offset`,`dataset` FROM `commits`".query[Commit].to[List]

    q.transact(xa).unsafeRunSync()
  }
  
  def allDatasets: Seq[Dataset] = { 
    val q = sql"SELECT `app`, `topic`, `dataset`, `step`, `commit` FROM `datasets`".query[Dataset].to[List]

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
  }
  
  private trait Table {
    def drop: ConnectionIO[Int]
    
    def create: ConnectionIO[Int]
  }
  
  private object Tables {
    val all: Seq[Table] = Seq(Commits, Datasets)
    
    object Commits extends Table {
      override val drop = sql"""
        DROP TABLE IF EXISTS `commits`
      """.update.run
      
      override val create = sql"""
        CREATE TABLE `commits` (
        `ID` int(11) NOT NULL AUTO_INCREMENT,
        `commit` int(64) NOT NULL,
        `topic` varchar(180) NOT NULL,
        `partition` int(11) NOT NULL,
        `offset` int(64) NOT NULL,
        `dataset` varchar(180) NOT NULL,
        `timestamp` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (`ID`),
        UNIQUE KEY `SOURCE_IDX` (`topic`,`dataset`)
      )""".update.run 
    }
    
    object Datasets extends Table {
      override val drop = sql"""
        DROP TABLE IF EXISTS `datasets`
      """.update.run
      
      override val create = sql"""
        CREATE TABLE `datasets` (
        `ID` int(11) NOT NULL AUTO_INCREMENT,
        `app` varchar(180) NOT NULL,
        `topic` varchar(180) NOT NULL,
        `dataset` varchar(180) NOT NULL,
        `step` varchar(180) NOT NULL,
        `commit` int(64) NOT NULL,
        PRIMARY KEY (`ID`),
        UNIQUE KEY `DATASET_IDX` (`app`,`topic`,`dataset`)
      )""".update.run
    }
  }
}
