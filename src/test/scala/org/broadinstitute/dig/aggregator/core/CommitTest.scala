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

  dbTest("datasetCommits - no ignoreProcessedBy") {
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

  dbTest("datasetCommits - with ignoreProcessedBy") {
    val c0 = makeCommit(0, topic = "x", dataset = "fooSet")
    val c1 = makeCommit(1, topic = "x", dataset = "fooSet")
    val c2 = makeCommit(2, topic = "x", dataset = "barSet")
    val c3 = makeCommit(3, topic = "y", dataset = "barSet")

    val d0 = Dataset(app = Some("fooApp"), topic = "x", dataset = "fooSet", commit = c0.commit)
    val d1 = Dataset(app = Some("barApp"), topic = "x", dataset = "fooSet", commit = c1.commit)
    val d2 = Dataset(app = Some("blergApp"), topic = "x", dataset = "barSet", commit = c2.commit)
    val d3 = Dataset(app = Some("blergApp"), topic = "y", dataset = "barSet", commit = c3.commit)

    insert(c0, c1, c2, c3)
    insert(d0, d1, d2, d3)

    val xs = Commit.commits(xa, "x", "fooApp").unsafeRunSync()

    assert(xs.toSet == Set(c1, c2))

    val ys = Commit.commits(xa, "y", "fooApp").unsafeRunSync()

    assert(ys == Seq(c3))

    assert(Commit.commits(xa, "z", "fooApp").unsafeRunSync().isEmpty)
    assert(Commit.commits(xa, "z", "barApp").unsafeRunSync().isEmpty)
    assert(Commit.commits(xa, "z", "blergApp").unsafeRunSync().isEmpty)
  }

  private def makeCommit(i: Int, topic: String, dataset: String): Commit =
    Commit(commit = i, topic = topic, partition = 123 + i, offset = 456L + i, dataset = dataset)

  private def makeCommit(i: Int, topic: String): Commit = makeCommit(i, topic, dataset = s"foo-$i")

  private def makeCommit(i: Int): Commit = makeCommit(i, s"asdf-$i", s"foo-$i")

}
