package org.broadinstitute.dig.aggregator.core

import org.scalatest.funsuite.AnyFunSuite

final class GlobTest extends AnyFunSuite {
  import Glob.String2Glob

  test("basic globs - should match") {
    val glob = "*/foo*/*/baz".toGlob

    assert(glob.matches("/foobar/ack/baz"))
    assert(glob.matches("some/foo/anything=here/baz"))
  }

  test("basic globs - should not match") {
    val glob = "*/foo*/*/baz".toGlob

    assert(!glob.matches("/foo"))
    assert(!glob.matches("bar/foo"))
    assert(!glob.matches("zoo/whee/foo"))
    assert(!glob.matches("/foobar//baz"))
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
    assert(glob.matches("anything/else/super/deep"))
  }

  test("false globs") {
    val glob = Glob.False

    assert(!glob.matches(""))
    assert(!glob.matches("anything"))
    assert(!glob.matches("anything/else/super/deep"))
  }

  test("unapply globs") {
    val glob  = "foo/bar=*/baz".toGlob
    val crazy = "*/wow=*/.../ancestor=*/*/this/...".toGlob

    "foo/bar=hello/baz" match {
      case glob(hello) => assert(hello == "hello")
      case _           => fail("glob pattern match failed")
    }

    "foo/wow=awesome/ignore/all/this/ancestor=child/again/this/ignore/even/more" match {
      case crazy(foo, awesome, child, again) =>
        assert(foo == "foo")
        assert(awesome == "awesome")
        assert(child == "child")
        assert(again == "again")
      case _ =>
        fail("crazy pattern match failed")
    }
  }
}
