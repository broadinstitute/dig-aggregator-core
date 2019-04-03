package org.broadinstitute.dig.aggregator.core

import org.scalatest.FunSuite

import scala.util._

/**
 * Tests for org.broadinstitute.dig.aggregator.core.emr._
 */
final class GlobTest extends FunSuite {
  import Glob.String2Glob

  test("basic globs - should match") {
    val glob = "*/foo*/*/baz".toGlob

    assert(glob.matches("/foobar/ack/baz") == true)
    assert(glob.matches("some/foo/anything=here/baz") == true)
  }

  test("basic globs - should not match") {
    val glob = "*/foo*/*/baz".toGlob

    assert(glob.matches("/foo") == false)
    assert(glob.matches("bar/foo") == false)
    assert(glob.matches("zoo/whee/foo") == false)
    assert(glob.matches("/foobar//baz") == false)
    assert(glob.matches("foo") == false)
    assert(glob.matches("ack/foo/bar") == false)
    assert(glob.matches("/foobar/ack/baz/whee") == false)
    assert(glob.matches("/foobar/ack/baz-whee") == false)
    assert(glob.matches("more/foo/anything/here/baz") == false)
    assert(glob.matches("prefix/more/foo/anything/here/baz") == false)
  }

  test("true globs") {
    val glob = Glob.True

    assert(glob.matches("") == true)
    assert(glob.matches("anything") == true)
    assert(glob.matches("anything/else/super/deep") == true)
  }

  test("false globs") {
    val glob = Glob.False

    assert(glob.matches("") == false)
    assert(glob.matches("anything") == false)
    assert(glob.matches("anything/else/super/deep") == false)
  }
}
