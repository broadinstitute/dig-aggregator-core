package org.broadinstitute.dig.aggregator.core.config

import org.scalatest.FunSuite
import org.json4s.Formats
import org.json4s.DefaultFormats

/**
 * @author clint
 * Sep 10, 2018
 */
final class DbsTest extends FunSuite {
  test("Deserialize JSON") {
    implicit val formats: Formats = DbType.formats
  
    import org.json4s.jackson.Serialization.read
    
    val json = """{ "read":"h2", "write":"mysql" }"""
    
    val dbs = read[Dbs](json)
    
    assert(dbs === Dbs(read = DbType.H2, write = DbType.MySql))
  }
}
