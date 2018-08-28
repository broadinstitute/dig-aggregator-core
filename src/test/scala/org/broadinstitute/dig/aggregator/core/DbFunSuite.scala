package org.broadinstitute.dig.aggregator.core

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
  
  import doobie._
  import doobie.implicits._
  import cats._
  import cats.implicits._
  import cats.data._
  import cats.effect.IO
  
  def allCommits: Seq[Commit] = { 
    val q = sql"SELECT `commit`,`topic`,`partition`,`offset`,`dataset` FROM `commits`".query[Commit].to[List]

    q.transact(xa).unsafeRunSync()
  }
  
  private def makeTables(): Unit = {
    makeCommits()
  }
  
  private def makeCommits(): Unit = {
    (dropCommits, createCommits).mapN(_ + _).transact(xa).unsafeRunSync()
  }
  
  private val dropCommits = { 
    sql"""
      DROP TABLE IF EXISTS `COMMITS`
    """.update.run
  }

  private val createCommits = {
    /*
     * commit: Long, // commits topic offset
    topic: String, // dataset topic
    partition: Int, // dataset topic partition
    offset: Long, // dataset topic offset
    dataset: String // dataset name
     */
    sql"""
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
}
