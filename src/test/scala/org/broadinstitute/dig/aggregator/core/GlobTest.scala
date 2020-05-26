package org.broadinstitute.dig.aggregator.core

import org.scalatest.funsuite.AnyFunSuite

final class GlobTest extends AnyFunSuite {
  import Implicits._

  test("should match") {
    val glob: Glob = "*/foo*/*/baz"

    assert(glob.matches("/foobar/ack/baz"))
    assert(glob.matches("some/foo/anything=here/baz"))
  }

  test("should not match") {
    val glob: Glob = "*/foo*/*/baz"

    assert(!glob.matches("/foo"))
    assert(!glob.matches("bar/foo"))
    assert(!glob.matches("zoo/whee/foo"))
    assert(!glob.matches("foo"))
    assert(!glob.matches("ack/foo/bar"))
    assert(!glob.matches("/foobar/ack/baz/whee"))
    assert(!glob.matches("/foobar/ack/baz-whee"))
    assert(!glob.matches("more/foo/anything/here/baz"))
    assert(!glob.matches("prefix/more/foo/anything/here/baz"))
  }

  test("true globs") {
    val glob = Glob.True

    assert(glob.matches(""))
    assert(glob.matches("anything"))

    // "*" only matches to a path separator
    assert(!glob.matches("anything/path"))
  }

  test("false globs") {
    val glob = Glob.False

    assert(!glob.matches(""))
    assert(!glob.matches("anything"))
    assert(!glob.matches("anything/else/super/deep"))
  }

  test("pattern matching globs") {
    val glob: Glob = "*/wow=*/ancestor=*/*/this"

    "foo/wow=awesome/ancestor=grandfather//this" match {
      case glob(start, wow, ancestor, end) =>
        assert(start == "foo")
        assert(wow == "awesome")
        assert(ancestor == "grandfather")
        assert(end.isEmpty)
    }
  }
}
