package org.broadinstitute.dig.aggregator.core

import org.scalatest.FunSuite
import java.time.Instant

/**
 * @author clint
 * Oct 1, 2018
 */
final class AnalysisProvenanceTest extends FunSuite {
  import Analysis.Provenance

  test("apply - no args") {
    val p = Provenance()
    
    assert(p.source === "https://example.com/downstream-app")
    assert(p.branch === "glerg")
    assert(p.commit === "nerg")
  }
  
  private val oct28th = Instant.parse("2017-10-29T18:52:40.889Z")
  
  test("apply - arbitrary versions") {
    val v = Versions(
        name = "blerg",
        version = "zerg",
        branch = "glerg",
        lastCommit = Some("nerg"),
        anyUncommittedChanges = false,
        describedVersion = Some("flerg"),
        buildDate = oct28th,
        remoteUrl = Some("https://example.com/downstream-app"))
    
    val p = Provenance(v)
    
    assert(p.source === "https://example.com/downstream-app")
    assert(p.branch === "glerg")
    assert(p.commit === "nerg")
  }
  
  test("apply - guards") {
    def makeVersions(lastCommit: Option[String], remoteUrl: Option[String]) = Versions(
        name = "blerg",
        version = "zerg",
        branch = "glerg",
        lastCommit = lastCommit,
        anyUncommittedChanges = false,
        describedVersion = Some("flerg"),
        buildDate = oct28th,
        remoteUrl = remoteUrl)
    
    intercept[Exception] {
      Provenance(makeVersions(None, None))
    }
    
    intercept[Exception] {
      Provenance(makeVersions(Some("sadasf"), None))
    }
    
    intercept[Exception] {
      Provenance(makeVersions(None, Some("asdgafd")))
    }
    
    Provenance(makeVersions(Some("asdasdasd"), Some("asdgafd")))
  }
}
