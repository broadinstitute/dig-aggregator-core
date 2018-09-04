package org.broadinstitute.dig.aggregator.core

/**
 * @author clint
 * Aug 28, 2018
 */
final class DatasetTest extends DbFunSuite {
  dbTest("insert") {
    val d0 = Dataset(app = "a0", topic = "t0", dataset = "d0", commit = 0L)
    val d1 = Dataset(app = "a1", topic = "t1", dataset = "d1", commit = 1L)
    val d2 = Dataset(app = "a2", topic = "t2", dataset = "d2", commit = 2L)
    
    assert(allDatasets.isEmpty)
    
    insert(d0)
    
    assert(allDatasets == Seq(d0))
    
    insert(d1, d2)
    
    assert(allDatasets.toSet == Set(d0, d1, d2))
  }
}
