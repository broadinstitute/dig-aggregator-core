package org.broadinstitute.dig.aggregator.core

import cats.effect.IO

import doobie._
import doobie.implicits._

import org.broadinstitute.dig.aggregator.core.processors.Processor

import org.scalatest.FunSuite

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

    insert(c0)

    assert(allCommits == Seq(c0))

    insert(c1, c2)

    assert(allCommits.toSet == Set(c0, c1, c2))
  }

  dbTest("insert - on duplicate key update") {
    val c0 = makeCommit(0)
    val c1 = Commit(commit = c0.commit + 1,
                    topic = c0.topic,
                    partition = c0.partition + 1,
                    offset = c0.offset + 1,
                    dataset = c0.dataset)

    assert(allCommits.isEmpty)

    insert(c0)

    assert(allCommits == Seq(c0))

    insert(c1)

    assert(allCommits == Seq(c1))
  }

  dbTest("commits - no ignoreProcessedBy") {
    val c0 = makeCommit(0, topic = "x")
    val c1 = makeCommit(1, topic = "x")
    val c2 = makeCommit(2, topic = "y")

    insert(c0, c1, c2)

    val xs = Commit.commits(xa, "x").unsafeRunSync()

    assert(xs.toSet == Set(c0, c1))

    val ys = Commit.commits(xa, "y").unsafeRunSync()

    assert(ys.toSet == Set(c2))

    val zs = Commit.commits(xa, "z").unsafeRunSync()

    assert(zs.isEmpty)
  }

  dbTest("commits - with ignoreProcessedBy") {
    val c0 = makeCommit(0, topic = "x", dataset = "fooSet")
    val c1 = makeCommit(1, topic = "x", dataset = "barSet")

    insert(c0, c1)

    val r0 = insertRun(TestProcessor.a, Seq("fooSet"), "fooSet-output")
    val r1 = insertRun(TestProcessor.b, Seq("barSet"), "barSet-output")

    // fooApp already processed fooSet, so only barSet needs processed
    val xs = Commit.commits(xa, "x", TestProcessor.a).unsafeRunSync()
    assert(xs.toSet == Set(c1))

    // barApp already processed barSet, so only needs fooSet
    val ys = Commit.commits(xa, "x", TestProcessor.b).unsafeRunSync()
    assert(ys == Seq(c0))

    // test against empty topic
    assert(Commit.commits(xa, "z", TestProcessor.a).unsafeRunSync().isEmpty)
    assert(Commit.commits(xa, "z", TestProcessor.b).unsafeRunSync().isEmpty)
  }

  private def makeCommit(i: Int, topic: String, dataset: String): Commit =
    Commit(commit = i, topic = topic, partition = 123 + i, offset = 456L + i, dataset = dataset)

  private def makeCommit(i: Int, topic: String): Commit = makeCommit(i, topic, dataset = s"foo-$i")

  private def makeCommit(i: Int): Commit = makeCommit(i, s"asdf-$i", s"foo-$i")

}
