package org.broadinstitute.dig.aggregator.core

import org.scalatest.funsuite.AnyFunSuite

final class ImplicitsTest extends AnyFunSuite {
  import Implicits._

  test("s3 key basename") {
    assert("foo/bar/baz".basename == "baz")
    assert("foo/".basename == "")
    assert("".basename == "")
  }

  test("s3 key dirname") {
    assert("foo/bar/baz".dirname == "foo/bar/")
    assert("foo/bar/baz/".dirname == "foo/bar/baz/")
    assert("foo".dirname == "")
    assert("".dirname == "")
  }

  test("s3 key common prefix") {
    assert("foo/bar/baz".commonPrefix == "foo/bar/")
    assert("foo/bar/baz/".commonPrefix == "foo/bar/baz/")
    assert("foo/bar=*/baz".commonPrefix == "foo/bar=")
    assert("foo".dirname == "")
    assert("".dirname == "")
  }
}
