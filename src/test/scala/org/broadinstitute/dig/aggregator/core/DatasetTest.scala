package org.broadinstitute.dig.aggregator.core

import scala.util._

/**
 * @author clint
 * Aug 28, 2018
 */
final class DatasetTest extends DbFunSuite {
  dbTest("insert") {
    val d0 = Dataset(app = Some("a0"), topic = "t0", dataset = "d0", commit = 0L)
    val d1 = Dataset(app = Some("a1"), topic = "t1", dataset = "d1", commit = 1L)
    val d2 = Dataset(app = Some("a2"), topic = "t2", dataset = "d2", commit = 2L)

    assert(allDatasets.isEmpty)

    insert(d0)

    assert(allDatasets == Seq(d0))

    insert(d1, d2)

    assert(allDatasets.toSet == Set(d0, d1, d2))
  }

  dbTest("insert - on duplicate key update") {
    val d0 = Dataset(app = Some("a0"), topic = "t0", dataset = "d0", commit = 0L)
    val d1 = Dataset(app = d0.app, topic = d0.topic, dataset = d0.dataset, commit = 1L)

    assert(allDatasets.isEmpty)

    insert(d0)

    assert(allDatasets == Seq(d0))

    insert(d1)

    assert(allDatasets.toSet == Set(d1))
  }

  dbTest("insert - fail to insert dataset with no app") {
    val d = Dataset(app = None, topic = "topic", dataset = "test", commit = 0L)

    Try(insert(d)) match {
      case Success(_) => throw new Exception("shouldn't be able to insert dataset with app=None")
      case Failure(_) => ()
    }
  }

  dbTest("datasets - lookup work to be done 1") {
    val d1 = Dataset(app = Some("a"), topic = "topic", dataset = "d0", commit = 0L)
    val d2 = Dataset(app = Some("a"), topic = "topic", dataset = "d1", commit = 1L)
    val d3 = Dataset(app = Some("a"), topic = "topic", dataset = "d2", commit = 2L)

    // datasets already processed by this app (b)
    val d4 = Dataset(app = Some("b"), topic = "topic", dataset = "d0", commit = 0L)
    val d5 = Dataset(app = Some("b"), topic = "topic", dataset = "d1", commit = 1L)
    val d6 = Dataset(app = Some("b"), topic = "topic", dataset = "d2", commit = 2L)

    // insert everything
    insert(d1)
    insert(d2)
    insert(d3)
    insert(d4)
    insert(d5)
    insert(d6)

    // find all the datasets b needs to process
    val datasets = Dataset.datasets(xa, "topic", "a", "b").unsafeRunSync

    assert(datasets.isEmpty)
  }

  dbTest("datasets - lookup work to be done 2") {
    val d1 = Dataset(app = Some("a"), topic = "topic", dataset = "d0", commit = 0L)
    val d2 = Dataset(app = Some("a"), topic = "topic", dataset = "d1", commit = 4L)
    val d3 = Dataset(app = Some("a"), topic = "topic", dataset = "d2", commit = 2L)

    // datasets already processed by this app (b)
    val d4 = Dataset(app = Some("b"), topic = "topic", dataset = "d0", commit = 0L)
    val d5 = Dataset(app = Some("b"), topic = "topic", dataset = "d1", commit = 1L)

    // insert everything
    insert(d1)
    insert(d2)
    insert(d3)
    insert(d4)
    insert(d5) // NOTE: commit 1 < commit 4 (from d2)

    // find all the datasets b needs to process
    val datasets = Dataset.datasets(xa, "topic", "a", "b").unsafeRunSync

    // should find 2 datasets that need processing
    assert(datasets.size == 2)
    assert(datasets.find(_.dataset == "d1").isDefined)
    assert(datasets.find(_.dataset == "d2").isDefined)
  }
}
