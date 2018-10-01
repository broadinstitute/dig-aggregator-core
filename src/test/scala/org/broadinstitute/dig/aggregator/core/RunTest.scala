package org.broadinstitute.dig.aggregator.core

import java.time.Instant

/**
 * @author clint
 * Oct 1, 2018
 */
final class RunTest extends DbFunSuite {
  private val oct28th = Instant.parse("2016-10-28T18:52:40.889Z")
  private val oct29th = Instant.parse("2016-10-29T18:52:40.889Z")
  
  private val app0 = "foo"
  private val app1 = "bar"
  
  private val ts0 = oct28th
  private val ts1 = oct29th
  
  private val data0 = "0-sdasdaskdljalsd"
  private val data1 = "1-ldkjhksdjskdjas"
  
  private val run0 = Run(app0, ts0, data0)
  private val run1 = Run(app1, ts1, data1)
  
  dbTest("insert") {
    assert(allRuns.isEmpty)
    
    insert(run1)
    
    assert(allRuns === Seq(run1))
    
    insert(run0)
    
    assert(allRuns.toSet === Set(run0, run1))
  }
  
  dbTest("lastRunForApp") {
    def lastRunForApp(appName: String): Option[Run] = Run.lastRunForApp(xa)(appName).unsafeRunSync() 
    
    assert(allRuns.isEmpty)
    assert(lastRunForApp(app0) === None)
    assert(lastRunForApp(app1) === None)
    
    insert(run0)
    
    assert(lastRunForApp(app0) === Some(run0))
    assert(lastRunForApp(app1) === None)
    
    insert(run1)
    
    assert(lastRunForApp(app0) === Some(run0))
    assert(lastRunForApp(app1) === Some(run1))
    
    val now = Instant.now
    
    val run0Prime = run0.copy(timestamp = now)
    
    insert(run0Prime)
    
    assert(lastRunForApp(app0) === Some(run0Prime))
    assert(lastRunForApp(app1) === Some(run1))
  }
}
