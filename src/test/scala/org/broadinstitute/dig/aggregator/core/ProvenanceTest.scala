package org.broadinstitute.dig.aggregator.core

import org.scalatest.funsuite.AnyFunSuite

final class ProvenanceTest extends AnyFunSuite {
  test("from resource") {
    val p = Provenance.fromResource("version.properties").get

    assert(p.source.contains("https://example.com/some-app"))
    assert(p.branch.contains("master"))
    assert(p.commit.contains("d4721d90e35e5a45645d92cf0b8a55da3c50855d"))
  }
}
