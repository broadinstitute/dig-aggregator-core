package org.broadinstitute.dig.aggregator.core

import org.scalatest.FunSuite

import cats.effect.IO
import doobie._
import doobie.implicits._


/**
 * @author clint
 * Aug 27, 2018
 */
final class CommitTest extends DbFunSuite {
  dbTest("insert") {
    val c0 = makeCommit(0)
    val c1 = makeCommit(1)
    val c2 = makeCommit(2)

    assert(allCommits.isEmpty)
    
    c0.insert(xa).unsafeRunSync()
    
    assert(allCommits.size == 1)
    assert(allCommits.head == c0)
    
    c1.insert(xa).unsafeRunSync()
    c2.insert(xa).unsafeRunSync()
    
    assert(allCommits.size == 3)
    assert(allCommits.toSet == Set(c0, c1, c2))
  }
  
  dbTest("datasets") {
    val c0 = makeCommit(0, "x")
    val c1 = makeCommit(1, "x")
    val c2 = makeCommit(2, "y")
    
    insert(c0, c1, c2)
    
    val xs = Commit.datasets(xa, "x").unsafeRunSync()
    
    assert(xs.toSet == Set(c0, c1))
    
    val ys = Commit.datasets(xa, "y").unsafeRunSync()
    
    assert(ys.toSet == Set(c2))
    
    val zs = Commit.datasets(xa, "z").unsafeRunSync()
    
    assert(zs.isEmpty)
  }

  private def makeCommit(i: Int, topic: String): Commit = Commit(
    commit = 123L + i,
    topic = topic,
    partition = 456 + i,
    offset = 999L + i,
    dataset = s"foo-$i")
    
  private def makeCommit(i: Int): Commit = makeCommit(i, s"asdf-$i")
  
  private def insert(cs: Commit*): Unit = {
    import cats.implicits._
    import cats.effect._
    
    cs.toList.map(_.insert(xa)).sequence.unsafeRunSync 
  }
}
