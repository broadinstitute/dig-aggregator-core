package org.broadinstitute.dig.aggregator.core.config

import org.scalatest.FunSuite
import org.json4s.Formats

/**
 * @author clint
 * Sep 10, 2018
 */
final class DbConfigTest extends FunSuite {
  test("Deserialize JSON") {
    import DbConfigTest.DbInfo
    
    val mysqlJson = """{
      "dbType":"mysql",
      "url": "http://example.com/mysql",
      "schema": "Foo-MySQL",
      "user": "foo-MySQL",
      "password": "bar-MySQL"
    }"""
    
    val h2Json = """{
      "dbType":"h2",
      "url": "http://example.com/h2",
      "schema": "Foo-H2",
      "user": "foo-H2",
      "password": "bar-H2"
    }"""
    
    val json = s"""{"mysql":${mysqlJson},"h2":${h2Json}}"""
    
    implicit val formats: Formats = DbConfig.formats + DbType.serializer
  
    import org.json4s.jackson.Serialization.read
    
    val dbInfo = read[DbInfo](json)
    
    val expected = DbInfo(
      DbConfig.H2(
        url = "http://example.com/h2",
        schema = "Foo-H2",
        user = "foo-H2",
        password = "bar-H2"),
      DbConfig.MySQL(
        url = "http://example.com/mysql",
        schema = "Foo-MySQL",
        user = "foo-MySQL",
        password = "bar-MySQL"))
        
    assert(dbInfo === expected)
  }
}

object DbConfigTest {
  private final case class DbInfo(h2: DbConfig, mysql: DbConfig)
}
