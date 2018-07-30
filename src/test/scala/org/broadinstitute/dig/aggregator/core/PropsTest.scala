package org.broadinstitute.dig.aggregator.core

import org.scalatest.FunSuite

/**
 * @author clint
 * Jul 30, 2018
 */
final class PropsTest extends FunSuite {
  test("Empty input") {
    assert(Props().isEmpty)
  }
  
  test("Non-empty input") {
    val props = Props("x" -> "1", "y" -> "abc", "z" -> "3")
    
    assert(props.getProperty("x") === "1")
    assert(props.getProperty("y") === "abc")
    assert(props.getProperty("z") === "3")
  }
}
